import logging

import pytest
from pss_fleet_data.core.exceptions import NonUniqueTimestampError

from fake_classes import FakePssFleetDataClient, create_fake_gdrive_file
from src.app.importer.import_worker import upload_collection
from src.app.models.queue_item import QueueItem


async def test_log_import_on_success(
    fake_pss_fleet_data_client: FakePssFleetDataClient,
    queue_item: QueueItem,
    caplog: pytest.LogCaptureFixture,
):
    queue_item.gdrive_file = create_fake_gdrive_file()

    with caplog.at_level(logging.INFO):
        await upload_collection(fake_pss_fleet_data_client, queue_item)

    assert "imported" in caplog.text.lower()


async def test_log_skip_on_non_unique_timestamp_error(
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
        await upload_collection(fake_pss_fleet_data_client, queue_item, reraise_non_unique_timestamp_error=False)

    assert "skipped" in caplog.text.lower()


async def test_raise_on_non_unique_timestamp_error_if_specified(
    fake_pss_fleet_data_client: FakePssFleetDataClient,
    queue_item: QueueItem,
    monkeypatch: pytest.MonkeyPatch,
):
    queue_item.gdrive_file = create_fake_gdrive_file()

    async def mock_upload_collection_raises_non_unique_timestamp_error(file_path, api_key=None):
        raise NonUniqueTimestampError(None, None, None, None, None, [])

    monkeypatch.setattr(
        fake_pss_fleet_data_client, FakePssFleetDataClient.upload_collection.__name__, mock_upload_collection_raises_non_unique_timestamp_error
    )

    with pytest.raises(NonUniqueTimestampError):
        await upload_collection(fake_pss_fleet_data_client, queue_item, reraise_non_unique_timestamp_error=True)


@pytest.mark.usefixtures("patch_sleep")
async def test_log_generic_exception_then_reraise(
    fake_pss_fleet_data_client: FakePssFleetDataClient,
    queue_item: QueueItem,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    exception = ValueError()

    queue_item.gdrive_file = create_fake_gdrive_file()

    async def mock_upload_collection_raises_non_unique_timestamp_error(file_path, api_key=None):
        raise exception

    monkeypatch.setattr(
        fake_pss_fleet_data_client, FakePssFleetDataClient.upload_collection.__name__, mock_upload_collection_raises_non_unique_timestamp_error
    )

    with caplog.at_level(logging.WARN):
        with pytest.raises(ValueError):
            await upload_collection(fake_pss_fleet_data_client, queue_item)

    assert "could not import file" in caplog.text.lower()
    assert type(exception).__qualname__ in caplog.text
