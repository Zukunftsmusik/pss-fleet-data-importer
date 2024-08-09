import pytest
from pydrive2.files import GoogleDriveFile

from src.app.core import config
from src.app.core.gdrive import GoogleDriveClient
from src.app.importer import Importer


class MockGoogleDriveClient(GoogleDriveClient):
    def __init__(self):
        pass


@pytest.fixture(scope="function")
def mock_gdrive_client() -> MockGoogleDriveClient:
    return MockGoogleDriveClient()


@pytest.fixture(scope="function")
def patch_gdrive_client_new(mock_gdrive_client: MockGoogleDriveClient, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(GoogleDriveClient, "__new__", mock_gdrive_client)


@pytest.fixture(scope="function")
def gdrive_file(gdrive_file_name: str) -> GoogleDriveFile:
    return GoogleDriveFile(None, {"title": gdrive_file_name})


@pytest.fixture(scope="session")
def gdrive_file_contents() -> str:
    return "gdrive_file_contents"


@pytest.fixture(scope="session")
def gdrive_file_name() -> str:
    return "gdrive_file_name"


@pytest.fixture(scope="function")
def patch_gdrive_client_get_file_contents(gdrive_file_contents: str, monkeypatch: pytest.MonkeyPatch):
    def mock_gdrive_client_get_file_contents(*args):
        return gdrive_file_contents

    monkeypatch.setattr(GoogleDriveClient, GoogleDriveClient.get_file_contents.__name__, mock_gdrive_client_get_file_contents)


@pytest.fixture(scope="function")
def importer(configuration: config.Config) -> Importer:
    return Importer(configuration, None, None, None)
