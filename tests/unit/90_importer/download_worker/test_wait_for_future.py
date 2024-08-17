from concurrent.futures import Future
from queue import Queue

import pytest

from src.app.core.models.cancellation_token import CancellationToken
from src.app.importer import download_worker
from src.app.importer.download_worker import wait_for_future
from src.app.models.queue_item import QueueItem


def test_cancel_future_if_not_done_and_cancel_token_cancelled(queue_item: QueueItem, cancel_token: CancellationToken):
    future = Future()
    cancel_token.cancel()

    wait_for_future(future, queue_item, None, None, None, None, cancel_token)

    assert future.cancelled() is True


def test_dont_cancel_future_if_done_and_cancel_token_cancelled(queue_item: QueueItem, cancel_token: CancellationToken):
    future = Future()
    future.set_result(None)

    cancel_token.cancel()

    wait_for_future(future, queue_item, None, None, None, None, cancel_token)

    assert future.cancelled() is False


def test_put_queue_item_in_queues_if_downloaded(queue_item: QueueItem, cancel_token: CancellationToken, monkeypatch: pytest.MonkeyPatch):
    def mock_wait_for_download_successful(future, inner_queue_item, executor, worker_timed_out_flag, timeout):
        queue_item.status.downloaded.value = True
        queue_item.status.download_error.value = False

    monkeypatch.setattr(download_worker, download_worker.wait_for_download.__name__, mock_wait_for_download_successful)

    database_queue = Queue()
    import_queue = Queue()

    wait_for_future(None, queue_item, None, database_queue, import_queue, None, cancel_token)

    assert database_queue.qsize() == 1
    assert import_queue.qsize() == 1


def test_dont_put_queue_item_in_queues_if_not_downloaded(queue_item: QueueItem, cancel_token: CancellationToken, monkeypatch: pytest.MonkeyPatch):
    def mock_wait_for_download_successful(future, inner_queue_item, executor, worker_timed_out_flag, timeout):
        queue_item.status.downloaded.value = False

    monkeypatch.setattr(download_worker, download_worker.wait_for_download.__name__, mock_wait_for_download_successful)

    database_queue = Queue()
    import_queue = Queue()

    wait_for_future(None, queue_item, None, database_queue, import_queue, None, cancel_token)

    assert database_queue.empty()
    assert import_queue.empty()
