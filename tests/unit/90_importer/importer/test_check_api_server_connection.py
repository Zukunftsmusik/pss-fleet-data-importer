import pytest
from httpx import ConnectError
from pss_fleet_data import PssFleetDataClient

from fake_classes import FakePssFleetDataClient
from src.app.importer import Importer


async def test_successful(fake_importer: Importer, monkeypatch: pytest.MonkeyPatch):
    async def mock_ping(*args):
        return

    monkeypatch.setattr(PssFleetDataClient, PssFleetDataClient.ping.__name__, mock_ping)

    result = await fake_importer.check_api_server_connection()
    assert result is True


async def test_unsuccessful(fake_importer: Importer, monkeypatch: pytest.MonkeyPatch):
    async def mock_ping(*args):
        raise ConnectError(None)

    monkeypatch.setattr(fake_importer.fleet_data_client, FakePssFleetDataClient.ping.__name__, mock_ping)

    result = await fake_importer.check_api_server_connection()
    assert result is False
