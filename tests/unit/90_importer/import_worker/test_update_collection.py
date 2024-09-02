import logging
from datetime import timedelta

import pytest
from pss_fleet_data import CollectionMetadata
from pss_fleet_data.core.exceptions import ConflictError

from fake_classes import FakePssFleetDataClient, create_fake_gdrive_file
from src.app.core import utils
from src.app.importer.import_worker import update_collection
from src.app.models.queue_item import QueueItem


def add_collection_to_be_updated(queue_item: QueueItem, fake_pss_fleet_data_client: FakePssFleetDataClient):
    timestamp = utils.get_next_full_hour(queue_item.gdrive_file.modified_date) - timedelta(minutes=1)
    fake_pss_fleet_data_client.collections[1] = CollectionMetadata(
        collection_id=1,
        timestamp=timestamp,
        duration=1337.0,
        fleet_count=100,
        user_count=8764,
        tournament_running=False,
        schema_version=9,
        max_tournament_battle_attempts=6,
        data_version=9,
    )


async def test_log_import_on_success(
    fake_pss_fleet_data_client: FakePssFleetDataClient,
    queue_item: QueueItem,
    caplog: pytest.LogCaptureFixture,
):
    queue_item.gdrive_file = create_fake_gdrive_file()
    add_collection_to_be_updated(queue_item, fake_pss_fleet_data_client)

    with caplog.at_level(logging.INFO):
        await update_collection(fake_pss_fleet_data_client, queue_item)

    assert "imported" in caplog.text.lower()


async def test_log_skip_on_conflict_error(
    fake_pss_fleet_data_client: FakePssFleetDataClient,
    queue_item: QueueItem,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    queue_item.gdrive_file = create_fake_gdrive_file()
    add_collection_to_be_updated(queue_item, fake_pss_fleet_data_client)

    async def mock_update_collection_raises_conflict_error(file_path, api_key=None):
        raise ConflictError(None, None, None, None, None, [])

    monkeypatch.setattr(fake_pss_fleet_data_client, FakePssFleetDataClient.update_collection.__name__, mock_update_collection_raises_conflict_error)

    with caplog.at_level(logging.INFO):
        await update_collection(fake_pss_fleet_data_client, queue_item)

    assert "skipped" in caplog.text.lower()


@pytest.mark.usefixtures("patch_sleep")
async def test_log_generic_exception_then_reraise(
    fake_pss_fleet_data_client: FakePssFleetDataClient,
    queue_item: QueueItem,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    exception = ValueError()

    queue_item.gdrive_file = create_fake_gdrive_file()
    add_collection_to_be_updated(queue_item, fake_pss_fleet_data_client)

    async def mock_update_collection_raises_non_unique_timestamp_error(file_path, api_key=None):
        raise exception

    monkeypatch.setattr(
        fake_pss_fleet_data_client, FakePssFleetDataClient.update_collection.__name__, mock_update_collection_raises_non_unique_timestamp_error
    )

    with caplog.at_level(logging.WARN):
        with pytest.raises(ValueError):
            await update_collection(fake_pss_fleet_data_client, queue_item)

    assert "could not import file" in caplog.text.lower()
    assert type(exception).__qualname__ in caplog.text
