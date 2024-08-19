import pytest
from pss_fleet_data import PssFleetDataClient

from src.app.core import config
from src.app.importer import Importer
from tests.fake_classes import FakePssFleetDataClient


@pytest.fixture(scope="function")
def fake_fleet_data_client() -> FakePssFleetDataClient:
    return FakePssFleetDataClient()


@pytest.fixture(scope="function")
def importer(configuration: config.Config, fake_fleet_data_client: FakePssFleetDataClient) -> Importer:
    return Importer(configuration, None, fake_fleet_data_client)
