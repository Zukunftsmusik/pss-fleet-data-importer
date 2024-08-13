import asyncio
import logging
import random
import string
import time
import uuid
from datetime import datetime

import googleapiclient.errors
import pydrive2.files
import pytest
from pydrive2.files import GoogleDriveFile

from mock_classes import MockGDriveFile, MockGoogleDriveClient
from src.app.core import config
from src.app.core.gdrive import GDriveFile
from src.app.database.models import CollectionFileDB
from src.app.models import CancellationToken
from src.app.models.queue_item import CollectionFileQueueItem


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
def configuration() -> config.Config:
    return config.ConfigRepository.get_config()


@pytest.fixture(scope="function")
def mock_gdrive_client() -> MockGoogleDriveClient:
    return MockGoogleDriveClient()


@pytest.fixture(scope="function")
def mock_gdrive_file(
    google_drive_file_id: str,
    google_drive_file_size: int,
    google_drive_file_content: str,
    google_drive_file_name: str,
    google_drive_file_modified_date: datetime,
) -> MockGDriveFile:
    return MockGDriveFile(
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
) -> GoogleDriveFile:
    return GoogleDriveFile(
        None,
        {
            "id": google_drive_file_id,
            "fileSize": str(google_drive_file_size),
            "title": google_drive_file_name,
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
def queue_item(gdrive_file, collection_file_db) -> CollectionFileQueueItem:
    return CollectionFileQueueItem(1, gdrive_file, collection_file_db, "/dev/null", None)
