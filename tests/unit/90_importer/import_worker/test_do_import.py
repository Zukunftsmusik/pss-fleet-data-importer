import logging

import pytest
from pss_fleet_data import ApiError
from pss_fleet_data.core.exceptions import NonUniqueTimestampError

from fake_classes import FakeFileSystem, FakePssFleetDataClient
from src.app.importer import import_worker
from src.app.importer.import_worker import do_import
from src.app.models.queue_item import QueueItem


@pytest.mark.usefixtures("patch_upload_collection_returns_timestamp")
async def test_delete_file_after_import_if_specified(
    fake_pss_fleet_data_client: FakePssFleetDataClient,
    queue_item: QueueItem,
    filesystem: FakeFileSystem,
):
    filesystem.write(queue_item.target_file_path, "abc")

    await do_import(fake_pss_fleet_data_client, queue_item, False, filesystem=filesystem)

    assert filesystem.exists(queue_item.target_file_path) == False


@pytest.mark.usefixtures("patch_upload_collection_returns_timestamp")
async def test_dont_delete_file_after_import_if_specified(
    fake_pss_fleet_data_client: FakePssFleetDataClient,
    queue_item: QueueItem,
    filesystem: FakeFileSystem,
):
    filesystem.write(queue_item.target_file_path, "abc")

    await do_import(fake_pss_fleet_data_client, queue_item, True, filesystem=filesystem)

    assert filesystem.exists(queue_item.target_file_path) == True


async def test_log_raised_api_error_on_upload_and_return(
    fake_pss_fleet_data_client: FakePssFleetDataClient,
    queue_item: QueueItem,
    filesystem: FakeFileSystem,
    api_error: ApiError,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    async def mock_upload_collection_returns_timestamp(
        fleet_data_client, inner_queue_item, import_attempts=2, reraise_non_unique_timestamp_error=False
    ):
        raise api_error

    monkeypatch.setattr(import_worker, import_worker.upload_collection.__name__, mock_upload_collection_returns_timestamp)

    filesystem.write(queue_item.target_file_path, "abc")

    with caplog.at_level(logging.ERROR):
        await do_import(fake_pss_fleet_data_client, queue_item, False, filesystem=filesystem)

    assert caplog.text


async def test_consider_file_as_imported_if_non_unique_timestamp_error_raised(
    fake_pss_fleet_data_client: FakePssFleetDataClient,
    queue_item: QueueItem,
    filesystem: FakeFileSystem,
    api_error: ApiError,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    async def mock_upload_collection_raises_non_unique_timestamp_error(
        fleet_data_client, inner_queue_item, import_attempts=2, reraise_non_unique_timestamp_error=False
    ):
        raise NonUniqueTimestampError(None, None, None, None, None, [])

    monkeypatch.setattr(import_worker, import_worker.upload_collection.__name__, mock_upload_collection_raises_non_unique_timestamp_error)

    filesystem.write(queue_item.target_file_path, "abc")

    await do_import(fake_pss_fleet_data_client, queue_item, False, filesystem=filesystem)

    assert filesystem.exists(queue_item.target_file_path) == False


async def test_update_file_on_non_unique_timestamp_error_if_specified(
    fake_pss_fleet_data_client: FakePssFleetDataClient,
    queue_item: QueueItem,
    filesystem: FakeFileSystem,
    api_error: ApiError,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    async def mock_upload_collection_raises_non_unique_timestamp_error(
        fleet_data_client, inner_queue_item, import_attempts=2, reraise_non_unique_timestamp_error=False
    ):
        raise NonUniqueTimestampError(None, None, None, None, None, [])

    async def mock_update_collection_succeeds(fleet_data_client, queue_item, import_attempts=2):
        return

    monkeypatch.setattr(import_worker, import_worker.upload_collection.__name__, mock_upload_collection_raises_non_unique_timestamp_error)
    monkeypatch.setattr(import_worker, import_worker.update_collection.__name__, mock_update_collection_succeeds)
    filesystem.write(queue_item.target_file_path, "abc")

    await do_import(fake_pss_fleet_data_client, queue_item, False, update_existing_collections=True, filesystem=filesystem)

    assert filesystem.exists(queue_item.target_file_path) == False


async def test_log_raised_api_error_on_update_and_return(
    fake_pss_fleet_data_client: FakePssFleetDataClient,
    queue_item: QueueItem,
    filesystem: FakeFileSystem,
    api_error: ApiError,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    async def mock_upload_collection_raises_non_unique_timestamp_error(
        fleet_data_client, inner_queue_item, import_attempts=2, reraise_non_unique_timestamp_error=False
    ):
        raise NonUniqueTimestampError(None, None, None, None, None, [])

    async def mock_update_collection_raises_api_error(fleet_data_client, queue_item, import_attempts=2):
        raise api_error

    monkeypatch.setattr(import_worker, import_worker.upload_collection.__name__, mock_upload_collection_raises_non_unique_timestamp_error)
    monkeypatch.setattr(import_worker, import_worker.update_collection.__name__, mock_update_collection_raises_api_error)

    filesystem.write(queue_item.target_file_path, "abc")

    with caplog.at_level(logging.ERROR):
        await do_import(fake_pss_fleet_data_client, queue_item, False, update_existing_collections=True, filesystem=filesystem)

    assert caplog.text
