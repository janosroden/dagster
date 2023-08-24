import hashlib
import json
import os
import sys

from dagster_externals import ExternalExecutionMessageWriter
from dagster_externals._protocol import ExternalExecutionMessage


def load_asset_value(asset_key: str, storage_path: str):
    with open(os.path.join(storage_path, asset_key), "r") as f:
        content = f.read()
        return json.loads(content)


def store_asset_value(asset_key: str, storage_path: str, value: int):
    with open(os.path.join(storage_path, asset_key), "w") as f:
        return f.write(json.dumps(value))


def compute_data_version(value: int):
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


class StdoutMessageSink(ExternalExecutionMessageWriter):
    def send_message(self, message: ExternalExecutionMessage) -> None:
        sys.stdout.writelines((json.dumps(message), "\n"))
