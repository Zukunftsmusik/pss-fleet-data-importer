import logging
from queue import Queue

import pytest
from pytest_mock import MockerFixture

from fake_classes import FakePssFleetDataClient
from src.app.importer import import_worker
from src.app.importer.import_worker import process_queue_item
from src.app.models.queue_item import QueueItem


async def test_wait_and_return_unaltered_none_count_on_empty_queue(fake_pss_fleet_data_client: FakePssFleetDataClient, mocker: MockerFixture):
    import_queue = Queue()
    database_queue = Queue()
    none_count = 0

    asyncio_sleep_mock = mocker.patch("asyncio.sleep")

    returned_none_count = await process_queue_item(fake_pss_fleet_data_client, import_queue, database_queue, none_count, False)

    assert asyncio_sleep_mock.call_count == 1
    assert returned_none_count == none_count
    assert database_queue.empty() is True


async def test_return_incremented_none_count_on_none_in_queue(fake_pss_fleet_data_client: FakePssFleetDataClient):
    import_queue = Queue()
    database_queue = Queue()
    none_count = 0

    import_queue.put(None)

    returned_none_count = await process_queue_item(fake_pss_fleet_data_client, import_queue, database_queue, none_count, False)

    assert returned_none_count == none_count + 1
    assert import_queue.empty() is True
    assert database_queue.empty() is True


async def test_only_log_error_on_queue_item_skipped(
    fake_pss_fleet_data_client: FakePssFleetDataClient,
    queue_item: QueueItem,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    def mock_skip_file_import_on_error_returns_true(queue_item, filesystem=None):
        return True

    monkeypatch.setattr(import_worker, import_worker.skip_file_import_on_error.__name__, mock_skip_file_import_on_error_returns_true)

    import_queue = Queue()
    database_queue = Queue()
    none_count = 0

    import_queue.put(queue_item)

    with caplog.at_level(logging.ERROR):
        returned_none_count = await process_queue_item(fake_pss_fleet_data_client, import_queue, database_queue, none_count, False)

    assert returned_none_count == none_count
    assert import_queue.empty() is True
    assert database_queue.empty() is True
    assert caplog.text


async def test_do_import_called_else(
    fake_pss_fleet_data_client: FakePssFleetDataClient,
    queue_item: QueueItem,
    monkeypatch: pytest.MonkeyPatch,
    mocker: MockerFixture,
):
    def mock_skip_file_import_on_error_returns_false(queue_item, filesystem=None):
        return False

    monkeypatch.setattr(import_worker, import_worker.skip_file_import_on_error.__name__, mock_skip_file_import_on_error_returns_false)

    do_import_mock = mocker.patch(f"src.app.importer.import_worker.{import_worker.do_import.__name__}")

    import_queue = Queue()
    database_queue = Queue()
    none_count = 0

    import_queue.put(queue_item)

    returned_none_count = await process_queue_item(fake_pss_fleet_data_client, import_queue, database_queue, none_count, False)

    assert do_import_mock.call_count == 1
    assert returned_none_count == none_count
