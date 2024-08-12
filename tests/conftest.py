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

from mock_classes import MockGDriveFile, MockGoogleDriveClient
from src.app.core import config
from src.app.models import CancellationToken


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
def random_gdrive_file() -> MockGDriveFile:
    size = random.randint(1024, 2048)
    character_space = string.digits + string.ascii_letters
    content = "".join(random.choice(character_space) for _ in range(size))
    return MockGDriveFile(uuid.uuid4(), "file_name", size, datetime(2024, 8, 1, 23, 59), content)
