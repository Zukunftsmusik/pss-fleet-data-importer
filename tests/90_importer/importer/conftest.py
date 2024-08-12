import pytest
from pss_fleet_data import PssFleetDataClient
from pydrive2.files import GoogleDriveFile

from mock_classes import MockPssFleetDataClient
from src.app.core import config
from src.app.core.gdrive import GoogleDriveClient
from src.app.importer import Importer


@pytest.fixture(scope="function")
def mock_fleet_data_client() -> MockPssFleetDataClient:
    return MockPssFleetDataClient()


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
def importer(configuration: config.Config, mock_fleet_data_client: PssFleetDataClient) -> Importer:
    return Importer(configuration, None, None, mock_fleet_data_client)
