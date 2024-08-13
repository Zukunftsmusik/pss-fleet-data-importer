import json
from pathlib import Path
from typing import Optional, Union

import yaml


class FileSystem:
    def exists(self, path: Union[Path, str]) -> bool:
        return Path(path).exists()

    def dump_json(self, path: Union[Path, str], content: dict, indent: Optional[int] = None):
        with open(path, "w") as fp:
            json.dump(content, fp, indent=indent)

    def dump_yaml(self, path: Union[Path, str], content: dict):
        with open(path, "w") as fp:
            yaml.dump(content, fp)

    def get_size(self, path: Union[Path, str]) -> int:
        return Path(path).stat().st_size

    def load_json(self, path: Union[Path, str]) -> dict:
        with open(path, "r") as fp:
            return json.load(fp)

    def read(self, path: Union[Path, str], mode: str = "r") -> str:
        with open(path, mode) as fp:
            return fp.read()

    def write(self, path: Union[Path, str], content: str, mode: str = "w"):
        with open(path, mode) as fp:
            fp.write(content)
