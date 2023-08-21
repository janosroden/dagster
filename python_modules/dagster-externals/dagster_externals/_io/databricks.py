import json
import os

from dagster_externals import (
    ExternalExecutionContextData,
    ExternalExecutionContextSource,
    ExternalExecutionMessage,
    ExternalExecutionMessageSink,
)

from .._protocol import DAGSTER_EXTERNALS_ENV_KEYS
from .._util import DagsterExternalsError, param_from_env


class DbfsContextSource(ExternalExecutionContextSource):
    def load_context(self) -> ExternalExecutionContextData:
        unmounted_path = self._get_path_from_env()
        path = os.path.join("/dbfs", unmounted_path.lstrip("/"))
        with open(path, "r") as f:
            return json.load(f)

    def _get_path_from_env(self) -> str:
        try:
            context_source_params = param_from_env("context_source")
            assert isinstance(context_source_params, dict)
            assert isinstance(context_source_params.get("path"), str)
            return context_source_params["path"]
        except AssertionError:
            raise DagsterExternalsError(
                f"`{self.__class__.__name__}` requires a `path` key in the"
                f" {DAGSTER_EXTERNALS_ENV_KEYS['context_source']} environment variable be a JSON"
                " object with a string `path` property."
            )


class DbfsMessageSink(ExternalExecutionMessageSink):
    def __init__(self):
        self._path = None
        self._counter = 1

    def send_message(self, message: ExternalExecutionMessage) -> None:
        message_path = os.path.join(self.path, f"{self._counter}.json")
        with open(message_path, "w") as f:
            f.write(json.dumps(message) + "\n")
        self._counter += 1

    @property
    def path(self) -> str:
        if self._path is None:
            unmounted_path = self._get_path_from_env()
            self._path = os.path.join("/dbfs", unmounted_path.lstrip("/"))
        return self._path

    def _get_path_from_env(self) -> str:
        try:
            context_source_params = param_from_env("message_sink")
            assert isinstance(context_source_params, dict)
            assert isinstance(context_source_params.get("path"), str)
            return context_source_params["path"]
        except AssertionError:
            raise DagsterExternalsError(
                f"`{self.__class__.__name__}` requires a `path` key in the"
                f" {DAGSTER_EXTERNALS_ENV_KEYS['message_sink']} environment variable be a JSON"
                " object with a string `path` property."
            )
