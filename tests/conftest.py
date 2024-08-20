import asyncio
import logging
import random
import string
import time
import uuid
from datetime import datetime
from hashlib import md5
from queue import Queue
from typing import Generator

import googleapiclient.errors
import pydrive2.files
import pytest
from pydrive2.files import GoogleDriveFile
from pytest_mock import MockerFixture
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlmodel.ext.asyncio.session import AsyncSession

from src.app.core.config import ConfigRepository
from src.app.core.gdrive import GDriveFile
from src.app.core.models.status import StatusFlag
from src.app.database.db_repository import DatabaseRepository
from src.app.database.models import *  # noqa: F403, F401
from src.app.models import CancellationToken
from src.app.models.queue_item import QueueItem
from tests.fake_classes import FakeConfig, FakeFileSystem, FakeGDriveFile, FakeGoogleDriveClient, FakePssFleetDataClient


class MockHttpResponse:
    reason: str = None
    status: int = 400


@pytest.fixture(scope="function")
def cancel_token() -> CancellationToken:
    return CancellationToken()


@pytest.fixture(scope="function")
def logger() -> logging.Logger:
    logging.basicConfig(level=0)
    return logging.getLogger("pytest")


@pytest.fixture(scope="function")
def google_api_http_error() -> googleapiclient.errors.HttpError:
    return googleapiclient.errors.HttpError(MockHttpResponse(), b"{}")


@pytest.fixture(scope="function")
def api_request_error(google_api_http_error: googleapiclient.errors.HttpError) -> pydrive2.files.ApiRequestError:
    return pydrive2.files.ApiRequestError(google_api_http_error)


@pytest.fixture(scope="function")
def patch_time_sleep(monkeypatch: pytest.MonkeyPatch):
    def mock_time_sleep(*args):
        pass

    monkeypatch.setattr(time, time.sleep.__name__, mock_time_sleep)


@pytest.fixture(scope="function")
def patch_asyncio_sleep(monkeypatch: pytest.MonkeyPatch):
    async def mock_asyncio_sleep(*args):
        pass

    monkeypatch.setattr(asyncio, asyncio.sleep.__name__, mock_asyncio_sleep)


@pytest.fixture(scope="function")
def google_api_errors(api_request_error: pydrive2.files.ApiRequestError) -> dict[type[Exception], Exception]:
    return {
        pydrive2.files.ApiRequestError: api_request_error,
        pydrive2.files.FileNotDownloadableError: pydrive2.files.FileNotDownloadableError(),
    }


@pytest.fixture(scope="function")
def fake_config(sqlite_file_name: str) -> FakeConfig:
    config = FakeConfig(f"sqlite+aiosqlite:///{sqlite_file_name}", f"sqlite:///{sqlite_file_name}")
    return config


@pytest.fixture(scope="function")
def fake_gdrive_client() -> FakeGoogleDriveClient:
    return FakeGoogleDriveClient()


@pytest.fixture(scope="function")
def fake_gdrive_file(
    google_drive_file_id: str,
    google_drive_file_size: int,
    google_drive_file_content: str,
    google_drive_file_name: str,
    google_drive_file_modified_date: datetime,
) -> FakeGDriveFile:
    return FakeGDriveFile(
        google_drive_file_id,
        google_drive_file_name,
        google_drive_file_size,
        google_drive_file_modified_date,
        google_drive_file_content,
    )


@pytest.fixture(scope="function")
def google_drive_file_content(google_drive_file_size: int) -> str:
    character_space = string.digits + string.ascii_letters
    return "".join(random.choice(character_space) for _ in range(google_drive_file_size))


@pytest.fixture(scope="function")
def google_drive_file_id() -> str:
    return str(uuid.uuid4())


@pytest.fixture(scope="function")
def google_drive_file_name() -> str:
    return "gdrive_file_name"


@pytest.fixture(scope="function")
def google_drive_file_size() -> int:
    return random.randint(1024, 2048)


@pytest.fixture(scope="function")
def google_drive_file_modified_date() -> datetime:
    return datetime(2024, 8, 1, 23, 59, 30)


@pytest.fixture(scope="function")
def google_drive_file_timestamp() -> datetime:
    return datetime(2024, 8, 1, 23, 59)


@pytest.fixture(scope="function")
def google_drive_file(
    google_drive_file_id: str,
    google_drive_file_size: int,
    google_drive_file_name: str,
    google_drive_file_modified_date: datetime,
    google_drive_file_content: str,
) -> GoogleDriveFile:
    return GoogleDriveFile(
        None,
        {
            "id": google_drive_file_id,
            "fileSize": str(google_drive_file_size),
            "title": google_drive_file_name,
            "md5Checksum": md5(google_drive_file_content.encode()).hexdigest(),
            "modifiedDate": google_drive_file_modified_date.isoformat(),
        },
        uploaded=True,
    )


@pytest.fixture(scope="function")
def gdrive_file(google_drive_file: GoogleDriveFile) -> GDriveFile:
    return GDriveFile(google_drive_file)


@pytest.fixture(scope="function")
def collection_file_db(
    google_drive_file_id: str,
    google_drive_file_name: str,
    google_drive_file_modified_date: datetime,
    google_drive_file_timestamp: datetime,
) -> CollectionFileDB:
    return CollectionFileDB(
        collection_file_id=1,
        gdrive_file_id=google_drive_file_id,
        file_name=google_drive_file_name,
        gdrive_modified_date=google_drive_file_modified_date,
        timestamp=google_drive_file_timestamp,
    )


@pytest.fixture(scope="function")
def queue_item(gdrive_file: GDriveFile, collection_file_db: CollectionFileDB, cancel_token: CancellationToken) -> QueueItem:
    return QueueItem(1, gdrive_file, collection_file_db, "/dev/null", cancel_token)


@pytest.fixture(scope="function")
def status_flag_false() -> StatusFlag:
    return StatusFlag("false", False)


@pytest.fixture(scope="function")
def status_flag_true() -> StatusFlag:
    return StatusFlag("true", True)


@pytest.fixture(scope="function")
def queue() -> Queue:
    return Queue()


@pytest.fixture(scope="function")
def filesystem() -> FakeFileSystem:
    return FakeFileSystem()


@pytest.fixture(scope="function")
def fake_queue_item(fake_gdrive_file: FakeGDriveFile, collection_file_db: CollectionFileDB) -> QueueItem:
    return QueueItem(1, fake_gdrive_file, collection_file_db, "/dev/null", None)


@pytest.fixture(scope="function")
def fake_pss_fleet_data_client() -> FakePssFleetDataClient:
    return FakePssFleetDataClient()


@pytest.fixture(scope="function")
def sqlite_file_name() -> str:
    return "pytest.sqlite"


@pytest.fixture(scope="function")
def create_sqlite_file(sqlite_file_name: str) -> AsyncEngine:
    async_engine = create_async_engine(f"sqlite+aiosqlite:///{sqlite_file_name}")
    SQLModel.metadata.create_all()
    return async_engine


@pytest.fixture(scope="function")
def session_factory(create_sqlite_file: AsyncEngine) -> Generator[async_sessionmaker[AsyncSession], None, None]:
    yield async_sessionmaker(bind=create_sqlite_file, class_=AsyncSession)


@pytest.fixture(scope="function")
def session(session_factory: async_sessionmaker[AsyncSession]):
    return session_factory()


@pytest.fixture(scope="function")
def patch_get_config_return_fake(fake_config: FakeConfig, monkeypatch: pytest.MonkeyPatch):
    def get_fake_config():
        return fake_config

    monkeypatch.setattr(ConfigRepository, ConfigRepository.get_config.__name__, get_fake_config)


@pytest.fixture(scope="function")
def patch_sleep(mocker: MockerFixture):
    mocker.patch(f"asyncio.sleep")
    mocker.patch(f"time.sleep")


@pytest.fixture(scope="function")
def reset_database_after_test():
    yield
    DatabaseRepository.clear_db()
