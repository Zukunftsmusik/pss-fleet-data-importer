import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Optional, Union

import yaml
from pss_fleet_data import PssFleetDataClient

from src.app.core.config import ConfigBase
from src.app.core.gdrive import GDriveFile, GoogleDriveClient


@dataclass(frozen=False)
class FakeConfig(ConfigBase):
    pass


class FakeGoogleDriveClient(GoogleDriveClient):
    def __init__(self):
        pass


class FakeGDriveFile(GDriveFile):
    def __init__(self, file_id: str, file_name: str, file_size: int, modified_date: datetime, content: str):
        self.id = file_id
        self.name = file_name
        self.size = file_size
        self.modified_date = modified_date
        self.content = content

    def get_content_string(self, *args: Any, **kwargs: Mapping[str, Any]):
        return self.content


class FakePssFleetDataClient(PssFleetDataClient):
    def __init__(self):
        pass


@dataclass(frozen=False)
class FakeStatResult:
    st_size: int = 0
    st_mode: int = 0


class FakeFileSystem:
    def __init__(self, files: Optional[dict[Union[Path, str], tuple[str, int]]] = None):
        files = files or {}
        self.__files: dict[Path, str] = {Path(path): content for path, content in files.items()}

    def exists(self, path: Union[Path, str]) -> bool:
        return Path(path) in self.__files.keys()

    def dump_json(self, path: Union[Path, str], content: dict, indent: Optional[int] = None):
        self.__files[path] = json.dumps(content, indent=indent)

    def dump_yaml(self, path: Union[Path, str], content: dict):
        self.__files[path] = yaml.dump(content)

    def get_size(self, path: Union[Path, str]) -> int:
        return len(self.read(path))

    def load_json(self, path: Union[Path, str]) -> dict:
        return json.loads(self.read(path))

    def read(self, path: Union[Path, str], _: str = "r") -> str:
        if self.exists(path):
            return self.__files[path]
        raise FileNotFoundError()

    def write(self, path: Union[Path, str], content: str, _: str = "w"):
        self.__files[Path(path)] = content
