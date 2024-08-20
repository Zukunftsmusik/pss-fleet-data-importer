import logging

import pytest
from pytest_mock import MockerFixture

from fake_classes import FakePssFleetDataClient
from src.app.importer import import_worker
from src.app.importer.import_worker import process_queue_item
from src.app.models.queue_item import QueueItem


async def test_only_log_error_on_queue_item_skipped(
    fake_pss_fleet_data_client: FakePssFleetDataClient,
    queue_item: QueueItem,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    def mock_skip_file_import_on_error_returns_true(queue_item, filesystem=None):
        return True

    monkeypatch.setattr(import_worker, import_worker.skip_file_import_on_error.__name__, mock_skip_file_import_on_error_returns_true)

    with caplog.at_level(logging.ERROR):
        await process_queue_item(queue_item, fake_pss_fleet_data_client, False)

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

    await process_queue_item(queue_item, fake_pss_fleet_data_client, False)

    assert do_import_mock.call_count == 1
