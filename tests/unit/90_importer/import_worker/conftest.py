from datetime import datetime

import pytest
from pss_fleet_data import ApiError, CollectionMetadata

from src.app.core import utils
from src.app.importer import import_worker


@pytest.fixture(scope="function")
def mock_collection_metadata():
    metadata = {
        "timestamp": datetime(2024, 6, 30, 7, 59, 0),
        "duration": 13.303961,
        "fleet_count": 100,
        "user_count": 8961,
        "tournament_running": True,
        "schema_version": 9,
        "max_tournament_battle_attempts": 6,
        "collection_id": 1337,
        "data_version": 9,
    }
    return CollectionMetadata(**metadata)


@pytest.fixture(scope="function")
def patch_upload_collection_returns_timestamp(monkeypatch: pytest.MonkeyPatch):
    async def mock_import_file_returns_timestamp(fleet_data_client, inner_queue_item, import_attempts=2, reraise_non_unique_timestamp_error=False):
        return utils.get_now()

    monkeypatch.setattr(import_worker, import_worker.upload_collection.__name__, mock_import_file_returns_timestamp)


@pytest.fixture(scope="function")
def api_error():
    return ApiError(None, None, None, None, None, [])
