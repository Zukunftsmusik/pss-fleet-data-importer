from concurrent.futures import ThreadPoolExecutor

import pytest

from src.app.core.models.cancellation_token import CancellationToken
from src.app.importer import download_worker
from src.app.importer.download_worker import DownloadFunction
from src.app.models.queue_item import QueueItem


@pytest.fixture(scope="function")
def thread_pool_executor_1() -> ThreadPoolExecutor:
    return ThreadPoolExecutor(1)


@pytest.fixture(scope="function")
def thread_pool_executor_2() -> ThreadPoolExecutor:
    return ThreadPoolExecutor(2)


@pytest.fixture(scope="function")
def thread_pool_executor_3() -> ThreadPoolExecutor:
    return ThreadPoolExecutor(3)


@pytest.fixture(scope="function")
def thread_pool_executor_4() -> ThreadPoolExecutor:
    return ThreadPoolExecutor(4)


@pytest.fixture(scope="function")
def thread_pool_executor_5() -> ThreadPoolExecutor:
    return ThreadPoolExecutor(5)


@pytest.fixture(scope="function")
def download_function() -> DownloadFunction:
    def func(queue_item: QueueItem, *args, cancel_token: CancellationToken, **kwargs):
        if cancel_token and cancel_token.cancelled:
            return None
        return queue_item

    return func


@pytest.fixture(scope="function")
def patch_download_gdrive_file_contents_return_something(google_drive_file_content: str, monkeypatch: pytest.MonkeyPatch):
    def mock_return_google_drive_file_content(gdrive_file, gdrive_client, cancel_token, item_no, max_download_attempts, log_stack_trace):
        return google_drive_file_content

    monkeypatch.setattr(download_worker, download_worker.download_gdrive_file_contents.__name__, mock_return_google_drive_file_content)


@pytest.fixture(scope="function")
def patch_write_gdrive_file_to_disk_pass(google_drive_file_content: str, monkeypatch: pytest.MonkeyPatch):
    def mock_pass(file_contents, file_path, cancel_token, item_no, gdrive_file_name, max_write_attempts):
        pass

    monkeypatch.setattr(download_worker, download_worker.write_gdrive_file_to_disk.__name__, mock_pass)


@pytest.fixture(scope="function")
def patch_file_already_exists_returns_false(monkeypatch: pytest.MonkeyPatch):
    def mock_return_false(queue_item, *, filesystem):
        return False

    monkeypatch.setattr(download_worker, download_worker.file_already_downloaded.__name__, mock_return_false)
