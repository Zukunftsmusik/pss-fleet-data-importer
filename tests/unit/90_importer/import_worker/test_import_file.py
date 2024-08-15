import logging
from datetime import datetime

import pytest
from pss_fleet_data import CollectionMetadata, PssFleetDataClient
from pss_fleet_data.core.exceptions import NonUniqueTimestampError

from fake_classes import FakePssFleetDataClient
from src.app.importer.import_worker import import_file
from src.app.models.queue_item import QueueItem


async def test_return_timestamp_and_log_import_on_success(
    fake_pss_fleet_data_client: FakePssFleetDataClient,
    queue_item: QueueItem,
    mock_collection_metadata: CollectionMetadata,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    async def mock_upload_collection_returns_mock_metadata(self, file_path, api_key=None):
        return mock_collection_metadata

    monkeypatch.setattr(PssFleetDataClient, PssFleetDataClient.upload_collection.__name__, mock_upload_collection_returns_mock_metadata)

    with caplog.at_level(logging.INFO):
        imported_at = await import_file(fake_pss_fleet_data_client, queue_item)

    assert isinstance(imported_at, datetime)
    assert "imported" in caplog.text.lower()


async def test_return_timestamp_and_log_skip_on_non_unique_timestamp_error(
    fake_pss_fleet_data_client: FakePssFleetDataClient,
    queue_item: QueueItem,
    mock_collection_metadata: CollectionMetadata,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    async def mock_upload_collection_raises_non_unique_timestamp_error(self, file_path, api_key=None):
        raise NonUniqueTimestampError(None, None, None, None, None, [])

    async def mock_get_most_recent_collection_metadata_by_timestamp_returns_mock_metadata(self, timestamp):
        return mock_collection_metadata

    monkeypatch.setattr(PssFleetDataClient, PssFleetDataClient.upload_collection.__name__, mock_upload_collection_raises_non_unique_timestamp_error)
    monkeypatch.setattr(
        PssFleetDataClient,
        PssFleetDataClient.get_most_recent_collection_metadata_by_timestamp.__name__,
        mock_get_most_recent_collection_metadata_by_timestamp_returns_mock_metadata,
    )

    with caplog.at_level(logging.INFO):
        imported_at = await import_file(fake_pss_fleet_data_client, queue_item)

    assert isinstance(imported_at, datetime)
    assert "skipped" in caplog.text.lower()
