import base64
import json
import os
import random
import string
import time
from contextlib import contextmanager
from threading import Event, Thread
from typing import Any, Iterator, Mapping, Optional

from dagster._core.execution.context.compute import OpExecutionContext
from dagster._core.external_execution.context import ExternalExecutionOrchestrationContext
from dagster._core.external_execution.resource import ExternalExecutionResource
from dagster_externals import (
    DAGSTER_EXTERNALS_ENV_KEYS,
    DagsterExternalsError,
    ExternalExecutionExtras,
    encode_param,
)
from databricks.sdk.service import files
from pydantic import Field

from dagster_databricks.databricks import DatabricksClient, DatabricksError, DatabricksJobRunner

# This has been manually uploaded to a test DBFS workspace.
DAGSTER_EXTERNALS_WHL_PATH = "dbfs:/FileStore/jars/dagster_externals-1!0+dev-py3-none-any.whl"

TASK_KEY = "DUMMY_TASK_KEY"

CLUSTER_DEFAULTS = {
    "size": {"num_workers": 0},
    "spark_version": "12.2.x-scala2.12",
    "nodes": {"node_types": {"node_type_id": "i3.xlarge"}},
}

_INPUT_FILENAME = "input"
_OUTPUT_DIRNAME = "output"


class DatabricksExecutionResource(ExternalExecutionResource):
    host: Optional[str] = Field(
        description="Databricks host, e.g. uksouth.azuredatabricks.com", default=None
    )
    token: Optional[str] = Field(description="Databricks access token", default=None)
    env: Optional[Mapping[str, str]] = Field(
        default=None,
        description="An optional dict of environment variables to pass to the subprocess.",
    )

    def run(
        self,
        script_path: str,
        git_url: str,
        git_branch: str,
        *,
        context: OpExecutionContext,
        extras: Optional[ExternalExecutionExtras] = None,
        host: Optional[str] = None,
        token: Optional[str] = None,
        env: Optional[Mapping[str, str]] = None,
    ) -> None:
        external_context = ExternalExecutionOrchestrationContext(context=context, extras=extras)
        client = DatabricksClient(host or self.host, token or self.token)
        with self._setup_io(external_context, client) as io_env:
            config = {
                "install_default_libraries": False,
                "libraries": [
                    {"whl": DAGSTER_EXTERNALS_WHL_PATH},
                ],
                # We need to set env vars so use a new cluster
                "cluster": {
                    "new": {
                        **CLUSTER_DEFAULTS,
                        "spark_env_vars": {
                            **self.get_base_env(),
                            **(env or {}),
                            **(self.env or {}),
                            **io_env,
                            "PYSPARK_PYTHON": "/databricks/python3/bin/python3",
                        },
                    }
                },
            }
            task = {
                "task_key": TASK_KEY,
                "spark_python_task": {
                    "python_file": script_path,
                    "source": "GIT",
                },
                "git_source": {
                    "git_url": git_url,
                    "git_branch": git_branch,
                },
            }

            runner = DatabricksJobRunner(
                host=host or self.host,
                token=token or self.token,
            )
            run_id = runner.submit_run(config, task)
            try:
                runner.client.wait_for_run_to_complete(
                    logger=context.log,
                    databricks_run_id=run_id,
                    poll_interval_sec=1,
                    max_wait_time_sec=60 * 60,
                )
            except DatabricksError as e:
                raise DagsterExternalsError(f"Error running Databricks job: {e}")

    # ########################
    # ##### IO
    # ########################

    @contextmanager
    def _setup_io(
        self, external_context: ExternalExecutionOrchestrationContext, client: DatabricksClient
    ) -> Iterator[Mapping[str, str]]:
        dbfs = files.DbfsAPI(client.workspace_client.api_client)
        with _dbfs_tempdir(dbfs) as tempdir, _dbfs_input(
            external_context, os.path.join(tempdir, _INPUT_FILENAME), dbfs
        ) as input_env, _dbfs_output(
            external_context, os.path.join(tempdir, _OUTPUT_DIRNAME), dbfs
        ) as output_env:
            yield {**input_env, **output_env}


@contextmanager
def _dbfs_tempdir(dbfs: files.DbfsAPI) -> Iterator[str]:
    dirname = "".join(random.choices(string.ascii_letters, k=30))
    tempdir = f"/tmp/{dirname}"
    dbfs.mkdirs(tempdir)
    try:
        yield tempdir
    finally:
        dbfs.delete(tempdir, recursive=True)


@contextmanager
def _dbfs_input(
    context: ExternalExecutionOrchestrationContext, path: str, dbfs: files.DbfsAPI
) -> Iterator[Mapping[str, str]]:
    context_source_params = {"path": path}
    env = {DAGSTER_EXTERNALS_ENV_KEYS["context_source"]: encode_param(context_source_params)}
    contents = base64.b64encode(json.dumps(context.get_data()).encode("utf-8")).decode("utf-8")
    dbfs.put(path, contents=contents)
    yield env
    try:
        dbfs.delete(path)
    except IOError:  # file not found
        pass


@contextmanager
def _dbfs_output(
    context: ExternalExecutionOrchestrationContext, path: str, dbfs: files.DbfsAPI
) -> Iterator[Mapping[str, str]]:
    message_sink_params = {"path": path}
    env = {DAGSTER_EXTERNALS_ENV_KEYS["message_sink"]: encode_param(message_sink_params)}
    is_task_complete = Event()
    thread = None
    try:
        dbfs.mkdirs(path)
        thread = Thread(target=_read_messages, args=(context, path, is_task_complete), daemon=True)
        thread.start()
        yield env
    finally:
        is_task_complete.set()
        if thread:
            thread.join()
        try:
            dbfs.delete(path, recursive=True)
        except IOError:  # file not found
            pass


def _read_messages(
    context: "ExternalExecutionOrchestrationContext",
    path: str,
    is_task_complete: Event,
    dbfs: files.DbfsAPI,
) -> Any:
    counter = 1
    while True:
        try:
            raw_message = dbfs.read(os.path.join(path, f"{counter}.json"))
            assert raw_message.data
            message = json.loads(raw_message.data)
            counter += 1
            context.handle_message(message)
        except IOError:
            if is_task_complete.is_set():
                break
            else:
                time.sleep(1)
