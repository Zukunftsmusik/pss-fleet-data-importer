import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Generator, Mapping, Optional, Union

import yaml
from pss_fleet_data import PssFleetDataClient
from pss_fleet_data.models.client_models import CollectionMetadata

from src.app.core import utils
from src.app.core.config import ConfigBase
from src.app.core.gdrive import GDriveFile, GoogleDriveClient


@dataclass(frozen=False)
class FakeConfig(ConfigBase):
    pass


class FakeGoogleDriveClient(GoogleDriveClient):
    def __init__(self):
        self.files: list[FakeGDriveFile] = []

    def list_files_by_modified_date(
        self,
        modified_after: Optional[datetime] = None,
        modified_before: Optional[datetime] = None,
    ) -> Generator[GDriveFile, None, None]:
        for f in (
            file
            for file in self.files
            if (not modified_after or file.modified_date > modified_after) and (not modified_before or file.modified_date < modified_before)
        ):
            yield f


class FakeGDriveFile(GDriveFile):
    def __init__(self, file_id: str, file_name: str, file_size: int, modified_date: datetime, content: str):
        self.id = file_id
        self.name = file_name
        self.size = file_size
        self.modified_date = modified_date
        self.content = content

    def get_content_string(self, mimetype: Optional[str] = None, encoding: str = "utf-8", remove_bom: bool = False):
        return self.content


class FakePssFleetDataClient(PssFleetDataClient):
    def __init__(self):
        self.collections: dict[int, CollectionMetadata] = {}

    async def ping(self) -> str:
        return "Pong!"

    async def upload_collection(self, file_path: Union[Path, str], api_key: Optional[str] = None) -> CollectionMetadata:
        file_name = Path(file_path).name
        collection_id = len(self.collections) + 1
        timestamp = utils.extract_timestamp_from_gdrive_file_name(file_name)

        metadata = CollectionMetadata(
            collection_id=collection_id,
            timestamp=timestamp,
            duration=5.0,
            fleet_count=1,
            user_count=1,
            tournament_running=False,
            data_version=9,
        )

        self.collections[collection_id] = metadata
        return metadata


class FakeFileSystem:
    def __init__(self, files: Optional[dict[Union[Path, str], tuple[str, int]]] = None):
        files = files or {}
        self.__files: dict[Path, str] = {Path(path): content for path, content in files.items()}

    def delete(self, path: Union[Path, str], *, missing_ok: bool = False):
        path = Path(path)
        if missing_ok:
            self.__files.pop(path, None)
        else:
            self.__files.pop(path)

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

    def mkdir(self, path: Union[Path, str]):
        self.write(path, None)

    def read(self, path: Union[Path, str], _: str = "r") -> str:
        path = Path(path)
        if self.exists(path):
            return self.__files[path]
        raise FileNotFoundError()

    def write(self, path: Union[Path, str], content: str, _: str = "w"):
        self.__files[Path(path)] = content
