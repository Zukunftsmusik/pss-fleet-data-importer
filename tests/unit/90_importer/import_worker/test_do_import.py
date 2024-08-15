import logging
from queue import Queue

import pytest
from pss_fleet_data import ApiError

from fake_classes import FakeFileSystem, FakePssFleetDataClient
from src.app.importer import import_worker
from src.app.importer.import_worker import do_import
from src.app.models.queue_item import QueueItem


@pytest.mark.usefixtures("patch_import_file_returns_timestamp")
async def test_put_item_in_database_queue_when_imported(
    fake_pss_fleet_data_client: FakePssFleetDataClient,
    queue_item: QueueItem,
    filesystem: FakeFileSystem,
):
    database_queue = Queue()
    filesystem.write(queue_item.target_file_path, "abc")

    await do_import(fake_pss_fleet_data_client, queue_item, database_queue, False, filesystem=filesystem)

    assert database_queue.qsize() == 1


@pytest.mark.usefixtures("patch_import_file_returns_timestamp")
async def test_delete_file_after_import_if_specified(
    fake_pss_fleet_data_client: FakePssFleetDataClient,
    queue_item: QueueItem,
    filesystem: FakeFileSystem,
):
    database_queue = Queue()
    filesystem.write(queue_item.target_file_path, "abc")

    await do_import(fake_pss_fleet_data_client, queue_item, database_queue, False, filesystem=filesystem)

    assert filesystem.exists(queue_item.target_file_path) == False


@pytest.mark.usefixtures("patch_import_file_returns_timestamp")
async def test_dont_delete_file_after_import_if_specified(
    fake_pss_fleet_data_client: FakePssFleetDataClient,
    queue_item: QueueItem,
    filesystem: FakeFileSystem,
):
    database_queue = Queue()
    filesystem.write(queue_item.target_file_path, "abc")

    await do_import(fake_pss_fleet_data_client, queue_item, database_queue, True, filesystem=filesystem)

    assert filesystem.exists(queue_item.target_file_path) == True


async def test_log_raised_api_error_and_return(
    fake_pss_fleet_data_client: FakePssFleetDataClient,
    queue_item: QueueItem,
    filesystem: FakeFileSystem,
    api_error: ApiError,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    async def mock_import_file_returns_timestamp(fleet_data_client, inner_queue_item):
        raise api_error

    monkeypatch.setattr(import_worker, import_worker.import_file.__name__, mock_import_file_returns_timestamp)

    database_queue = Queue()
    filesystem.write(queue_item.target_file_path, "abc")

    with caplog.at_level(logging.ERROR):
        await do_import(fake_pss_fleet_data_client, queue_item, database_queue, False, filesystem=filesystem)

    assert caplog.text
