import pytest
from httpx import ConnectError
from pss_fleet_data import PssFleetDataClient

from src.app.importer import Importer


async def test_successful(importer: Importer, monkeypatch: pytest.MonkeyPatch):
    async def mock_ping(*args):
        return

    monkeypatch.setattr(PssFleetDataClient, PssFleetDataClient.ping.__name__, mock_ping)

    result = await importer.check_api_server_connection()
    assert result is True


async def test_unsuccessful(importer: Importer, monkeypatch: pytest.MonkeyPatch):
    async def mock_ping(*args):
        raise ConnectError(None)

    monkeypatch.setattr(PssFleetDataClient, PssFleetDataClient.ping.__name__, mock_ping)

    result = await importer.check_api_server_connection()
    assert result is False
