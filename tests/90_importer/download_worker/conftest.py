from concurrent.futures import ThreadPoolExecutor

import pytest

from src.app.core.models.cancellation_token import CancellationToken
from src.app.importer.download_worker import DownloadFunction
from src.app.models.queue_item import CollectionFileQueueItem


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


@pytest.fixture()
def download_function() -> DownloadFunction:
    def func(queue_item: CollectionFileQueueItem, *args, cancel_token: CancellationToken, **kwargs):
        if cancel_token and cancel_token.cancelled:
            return None
        return queue_item

    return func
