import json
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import md5
from pathlib import Path
from typing import Generator, Optional, Union

import yaml
from pss_fleet_data.models.client_models import CollectionMetadata

from src.app.core import utils
from src.app.core.config import ConfigBase
from src.app.core.gdrive import GDriveFile


@dataclass(frozen=False)
class FakeConfig(ConfigBase):
    pass


class FakeGDriveFile:
    def __init__(self, file_id: str, file_name: str, file_size: int, modified_date: datetime, content: str, get_content_exception: Exception = None):
        self.id = file_id
        self.name = file_name
        self.size = file_size
        self.modified_date = modified_date
        self.content = content
        self.md5_checksum = md5(content.encode()).hexdigest()
        self.exception = get_content_exception

    def get_content_string(self, mimetype: Optional[str] = None, encoding: str = "utf-8", remove_bom: bool = False):
        if self.exception:
            raise self.exception

        return self.content


class FakeGoogleDriveClient:
    def __init__(self):
        self.files: list[Union[FakeGDriveFile, GDriveFile]] = []

    def list_files_by_modified_date(
        self,
        modified_after: Optional[datetime] = None,
        modified_before: Optional[datetime] = None,
    ) -> Generator[Union[FakeGDriveFile, GDriveFile], None, None]:
        for f in (
            file
            for file in self.files
            if (not modified_after or file.modified_date > modified_after) and (not modified_before or file.modified_date < modified_before)
        ):
            yield f


class FakePssFleetDataClient:
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
            schema_version=9,
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


def create_fake_gdrive_file(file_id: Optional[str] = None, file_name: Optional[str] = None, modified_date: Optional[datetime] = None):
    if modified_date:
        timestamp = modified_date.replace(second=0)
    else:
        timestamp = datetime.fromordinal(random.randint(737342, 739129)).replace(hour=23, minute=59)

    file_id = file_id or str(uuid.uuid4())
    file_name = file_name or timestamp.strftime("pss-top-100_%Y%m%d-%H%M%S.json")
    modified_date = modified_date or timestamp + timedelta(seconds=30)

    content = json.dumps(
        {
            "meta": {
                "timestamp": timestamp.isoformat().replace("T", ""),
                "duration": 5.0,
                "fleet_count": 0,
                "user_count": 0,
                "tournament_running": True,
                "schema_version": 9,
                "max_tournament_battle_attempts": 6,
            },
            "fleets": [],
            "users": [],
        }
    )
    file_size = len(content)

    return FakeGDriveFile(file_id, file_name, file_size, modified_date, content)
