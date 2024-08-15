from datetime import datetime

import pytest
from pss_fleet_data import CollectionMetadata


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
