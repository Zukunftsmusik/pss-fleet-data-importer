import pytest
from pss_fleet_data import PssFleetDataClient

from src.app.core import config
from src.app.importer import Importer
from tests.fake_classes import FakePssFleetDataClient


@pytest.fixture(scope="function")
def mock_fleet_data_client() -> FakePssFleetDataClient:
    return FakePssFleetDataClient()


@pytest.fixture(scope="function")
def importer(configuration: config.Config, mock_fleet_data_client: PssFleetDataClient) -> Importer:
    return Importer(configuration, None, mock_fleet_data_client)
