from unittest import mock

import pytest

from tests.fake_classes import FakeConfig, FakeFileSystem, FakeGoogleDriveClient, FakeImporter, FakePssFleetDataClient, FakeUnitOfWork


@pytest.fixture(scope="function")
def fake_fleet_data_client() -> FakePssFleetDataClient:
    return FakePssFleetDataClient()


@pytest.fixture(scope="function")
def fake_importer(
    fake_config: FakeConfig,
    fake_gdrive_client: FakeGoogleDriveClient,
    fake_fleet_data_client: FakePssFleetDataClient,
    filesystem: FakeFileSystem,
) -> FakeImporter:

    return FakeImporter(fake_config, fake_gdrive_client, fake_fleet_data_client, filesystem=filesystem)


@pytest.fixture(scope="function")
def patch_unit_of_work_use_fake():
    with mock.patch("src.app.importer.importer.SqlModelUnitOfWork", new=FakeUnitOfWork):
        yield
