import copy
import io
from pathlib import Path
from typing import List

from ruamel.yaml import YAML


def read_yaml(file_path: Path) -> dict:
    yaml = YAML()
    yaml.preserve_quotes = True
    return yaml.load(file_path)


def write_yaml(input: dict | List, file_path: Path):
    data = copy.deepcopy(input)
    yaml = YAML()
    buffer = io.BytesIO()
    yaml.dump(data, buffer)
    with file_path.open("w") as file:
        file.write(buffer.getvalue().decode("utf-8"))
