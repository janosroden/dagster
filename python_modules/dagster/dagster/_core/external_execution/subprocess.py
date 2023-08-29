import os
import tempfile
from contextlib import ExitStack, contextmanager
from subprocess import Popen
from typing import Iterator, Mapping, Optional, Sequence, Union

from dagster_externals import (
    DAGSTER_EXTERNALS_ENV_KEYS,
    ExternalExecutionExtras,
    encode_env_var,
)
from pydantic import Field

from dagster._core.errors import DagsterExternalExecutionError
from dagster._core.execution.context.compute import OpExecutionContext
from dagster._core.external_execution.context import (
    ExternalExecutionOrchestrationContext,
)
from dagster._core.external_execution.resource import (
    ExternalExecutionContextInjector,
    ExternalExecutionMessageReader,
    ExternalExecutionResource,
)
from dagster._core.external_execution.utils import (
    get_file_context_injector,
    get_file_message_reader,
)

_CONTEXT_INJECTOR_FILENAME = "context"
_MESSAGE_READER_FILENAME = "messages"


class SubprocessExecutionResource(ExternalExecutionResource):
    cwd: Optional[str] = Field(
        default=None, description="Working directory in which to launch the subprocess command."
    )
    env: Optional[Mapping[str, str]] = Field(
        default=None,
        description="An optional dict of environment variables to pass to the subprocess.",
    )

    def run(
        self,
        command: Union[str, Sequence[str]],
        *,
        context: OpExecutionContext,
        extras: Optional[ExternalExecutionExtras] = None,
        context_injector: Optional[ExternalExecutionContextInjector] = None,
        message_reader: Optional[ExternalExecutionMessageReader] = None,
        env: Optional[Mapping[str, str]] = None,
        cwd: Optional[str] = None,
    ) -> None:
        external_context = ExternalExecutionOrchestrationContext(context=context, extras=extras)
        with self._setup_io(external_context, context_injector, message_reader) as io_env:
            process = Popen(
                command,
                cwd=cwd or self.cwd,
                env={
                    **self.get_base_env(),
                    **{**(env or {}), **(self.env or {})},
                    **io_env,
                },
            )
            process.wait()

            if process.returncode != 0:
                raise DagsterExternalExecutionError(
                    f"External execution process failed with code {process.returncode}"
                )

    @contextmanager
    def _setup_io(
        self,
        external_context: ExternalExecutionOrchestrationContext,
        context_injector: Optional[ExternalExecutionContextInjector],
        message_reader: Optional[ExternalExecutionMessageReader],
    ) -> Iterator[Mapping[str, str]]:
        with ExitStack() as stack:
            if context_injector is None or message_reader is None:
                tempdir = stack.enter_context(tempfile.TemporaryDirectory())
                context_injector = context_injector or get_file_context_injector(
                    os.path.join(tempdir, _CONTEXT_INJECTOR_FILENAME)
                )
                message_reader = message_reader or get_file_message_reader(
                    os.path.join(tempdir, _MESSAGE_READER_FILENAME)
                )
            context_injector_params = stack.enter_context(context_injector(external_context))
            message_reader_params = stack.enter_context(message_reader(external_context))
            io_env = {
                DAGSTER_EXTERNALS_ENV_KEYS["context"]: encode_env_var(context_injector_params),
                DAGSTER_EXTERNALS_ENV_KEYS["messages"]: encode_env_var(message_reader_params),
            }
            yield io_env
