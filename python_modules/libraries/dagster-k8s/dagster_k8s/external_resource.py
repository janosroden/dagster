import json
import random
import string
from typing import Mapping, Optional, Sequence, Union

import kubernetes
from dagster import OpExecutionContext
from dagster._core.external_execution.context import (
    ExternalExecutionOrchestrationContext,
)
from dagster._core.external_execution.resource import (
    ExternalExecutionResource,
)
from dagster_externals import (
    DAGSTER_EXTERNALS_ENV_KEYS,
    ExternalExecutionExtras,
)

from .client import DagsterKubernetesClient, WaitForPodState


def get_pod_name(run_id: str, op_name: str):
    clean_op_name = op_name.replace("_", "-")
    suffix = "".join(random.choice(string.digits) for i in range(10))
    return f"dagster-{run_id[:18]}-{clean_op_name[:20]}-{suffix}"


DEFAULT_TIMEOUT = 30


class K8sPodExecutionResource(ExternalExecutionResource):
    # config?

    def run(
        self,
        context: OpExecutionContext,
        image: str,
        command: Union[str, Sequence[str]],
        namespace: Optional[str],
        env: Mapping[str, str],
        extras: Optional[ExternalExecutionExtras] = None,
    ) -> None:
        client = DagsterKubernetesClient.production_client()

        external_context = ExternalExecutionOrchestrationContext(context=context, extras=extras)

        namespace = namespace or "default"
        pod_name = get_pod_name(context.run_id, context.op.name)
        context_config_map_name = pod_name

        io_env = {
            DAGSTER_EXTERNALS_ENV_KEYS["context_source"]: json.dumps(
                {"path": "/mnt/dagster/context.json"}
            ),
            "STDOUT_MSG_SINK": "1",
        }

        context_config_map_body = kubernetes.client.V1ConfigMap(
            metadata=kubernetes.client.V1ObjectMeta(
                name=context_config_map_name,
            ),
            data={
                "context.json": json.dumps(external_context.get_data()),
            },
        )
        client.core_api.create_namespaced_config_map(namespace, context_config_map_body)

        pod_body = kubernetes.client.V1Pod(
            metadata=kubernetes.client.V1ObjectMeta(
                name=pod_name,
            ),
            spec=kubernetes.client.V1PodSpec(
                restart_policy="Never",
                volumes=[
                    {
                        "name": "dagster-externals-context",
                        "configMap": {
                            "name": context_config_map_name,
                        },
                    }
                ],
                containers=[
                    kubernetes.client.V1Container(
                        name="execution",
                        image=image,
                        command=command,
                        env=[
                            {"name": k, "value": v}
                            for k, v in {
                                **self.get_base_env(),
                                **io_env,
                                **env,
                            }.items()
                        ],
                        volume_mounts=[
                            {
                                "mountPath": "/mnt/dagster/",
                                "name": "dagster-externals-context",
                            }
                        ],
                    )
                ],
            ),
        )

        client.core_api.create_namespaced_pod(namespace, pod_body)
        try:
            client.wait_for_pod(
                pod_name,
                namespace,
                wait_for_state=WaitForPodState.Ready,
            )
            for line in client.core_api.read_namespaced_pod_log(
                pod_name,
                namespace,
                follow=True,
                _preload_content=False,  # avoid JSON processing
            ).stream():
                log_chunk = line.decode("utf-8")
                for log_line in log_chunk.split("\n"):
                    # exceptions as control flow, you love to see it
                    try:
                        message = json.loads(log_line)
                        # need better message check
                        assert message.keys() == {"method", "params"}
                        external_context.handle_message(message)
                    except Exception:
                        # move non-message logs in to stdout for compute log capture
                        print(log_line)  # noqa: T201

        finally:
            client.core_api.delete_namespaced_config_map(context_config_map_name, namespace)
            client.core_api.delete_namespaced_pod(pod_name, namespace)
