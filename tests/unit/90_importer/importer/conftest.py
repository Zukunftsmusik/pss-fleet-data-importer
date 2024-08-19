import pytest

from src.app.core import config
from tests.fake_classes import FakeFileSystem, FakeGoogleDriveClient, FakeImporter, FakePssFleetDataClient


@pytest.fixture(scope="function")
def fake_fleet_data_client() -> FakePssFleetDataClient:
    return FakePssFleetDataClient()


@pytest.fixture(scope="function")
def fake_importer(
    configuration: config.Config,
    fake_gdrive_client: FakeGoogleDriveClient,
    fake_fleet_data_client: FakePssFleetDataClient,
    filesystem: FakeFileSystem,
) -> FakeImporter:
    return FakeImporter(configuration, fake_gdrive_client, fake_fleet_data_client, filesystem=filesystem)
