from concurrent.futures import CancelledError, Future, ThreadPoolExecutor

import pytest

from src.app.core.models.cancellation_token import OperationCancelledError
from src.app.importer.download_worker import wait_for_download
from src.app.importer.exceptions import DownloadFailedError
from src.app.models import QueueItem


def test_set_downloaded_true_error_false_and_return_queue_item_on_success(queue_item: QueueItem, monkeypatch: pytest.MonkeyPatch):
    def mock_future_result_returns(self, timeout=None):
        return

    monkeypatch.setattr(Future, Future.result.__name__, mock_future_result_returns)

    returned_queue_item = wait_for_download(Future(), queue_item, None)

    assert returned_queue_item.status.downloaded.value is True
    assert returned_queue_item.status.download_error.value is False
    assert id(returned_queue_item) == id(queue_item)


def test_set_downloaded_false_error_false_on_future_cancelled(queue_item: QueueItem, monkeypatch: pytest.MonkeyPatch):
    def mock_future_cancelled(self, timeout=None):
        raise CancelledError()

    monkeypatch.setattr(Future, Future.result.__name__, mock_future_cancelled)

    returned_queue_item = wait_for_download(Future(), queue_item, None)

    assert returned_queue_item.status.downloaded.value is False
    assert returned_queue_item.status.download_error.value is False


def test_set_downloaded_false_error_false_on_cancel_token_cancelled(queue_item: QueueItem, monkeypatch: pytest.MonkeyPatch):
    def mock_future_result_cancel_token_cancelled(self, timeout=None):
        raise OperationCancelledError()

    monkeypatch.setattr(Future, Future.result.__name__, mock_future_result_cancel_token_cancelled)

    returned_queue_item = wait_for_download(Future(), queue_item, None)

    assert returned_queue_item.status.downloaded.value is False
    assert returned_queue_item.status.download_error.value is False


def test_set_downloaded_false_error_true_on_download_failed_error(queue_item: QueueItem, monkeypatch: pytest.MonkeyPatch):
    def mock_future_result_raises_download_error(self, timeout=None):
        raise DownloadFailedError(queue_item.gdrive_file.name, "whatever")

    monkeypatch.setattr(Future, Future.result.__name__, mock_future_result_raises_download_error)

    returned_queue_item = wait_for_download(Future(), queue_item, None)

    assert returned_queue_item.status.downloaded.value is False
    assert returned_queue_item.status.download_error.value is True


def test_set_downloaded_false_error_true_timeout_flag_true_shutdown_executor_on_timeout_error(
    queue_item: QueueItem, thread_pool_executor_1: ThreadPoolExecutor, monkeypatch: pytest.MonkeyPatch
):
    def mock_future_result_cancelled(self, timeout=None):
        raise TimeoutError()

    monkeypatch.setattr(Future, Future.result.__name__, mock_future_result_cancelled)

    returned_queue_item = wait_for_download(Future(), queue_item, thread_pool_executor_1)

    assert returned_queue_item.status.downloaded.value is False
    assert returned_queue_item.status.download_error.value is True
    assert thread_pool_executor_1._shutdown is True
