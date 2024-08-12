import pytest
from pss_fleet_data import PssFleetDataClient

from mock_classes import MockPssFleetDataClient
from src.app.core import config
from src.app.importer import Importer


@pytest.fixture(scope="function")
def mock_fleet_data_client() -> MockPssFleetDataClient:
    return MockPssFleetDataClient()


@pytest.fixture(scope="function")
def importer(configuration: config.Config, mock_fleet_data_client: PssFleetDataClient) -> Importer:
    return Importer(configuration, None, None, mock_fleet_data_client)
