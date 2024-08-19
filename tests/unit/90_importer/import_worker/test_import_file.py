import logging

import pytest
from pss_fleet_data.core.exceptions import NonUniqueTimestampError

from fake_classes import FakePssFleetDataClient, create_fake_gdrive_file
from src.app.importer.import_worker import import_file
from src.app.models.queue_item import QueueItem


async def test_log_import_on_success(
    fake_pss_fleet_data_client: FakePssFleetDataClient,
    queue_item: QueueItem,
    caplog: pytest.LogCaptureFixture,
):
    queue_item.gdrive_file = create_fake_gdrive_file()

    with caplog.at_level(logging.INFO):
        await import_file(fake_pss_fleet_data_client, queue_item)

    assert "imported" in caplog.text.lower()


async def test_return_timestamp_and_log_skip_on_non_unique_timestamp_error(
    fake_pss_fleet_data_client: FakePssFleetDataClient,
    queue_item: QueueItem,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    queue_item.gdrive_file = create_fake_gdrive_file()

    async def mock_upload_collection_raises_non_unique_timestamp_error(file_path, api_key=None):
        raise NonUniqueTimestampError(None, None, None, None, None, [])

    monkeypatch.setattr(
        fake_pss_fleet_data_client, FakePssFleetDataClient.upload_collection.__name__, mock_upload_collection_raises_non_unique_timestamp_error
    )

    with caplog.at_level(logging.INFO):
        await import_file(fake_pss_fleet_data_client, queue_item)

    assert "skipped" in caplog.text.lower()
