from concurrent.futures import ThreadPoolExecutor
from itertools import repeat

import pytest

from src.app.core.models.cancellation_token import CancellationToken
from src.app.importer.download_worker import DownloadFunction, setup_futures
from src.app.models.queue_item import CollectionFileQueueItem


test_cases_input_count = [pytest.param(i, id=str(i)) for i in range(1, 11)]


@pytest.mark.parametrize(["input_count"], test_cases_input_count)
def test_output_count_equals_input_count(
    queue_item: CollectionFileQueueItem,
    thread_pool_executor_1: ThreadPoolExecutor,
    download_function: DownloadFunction,
    cancel_token: CancellationToken,
    input_count: int,
):
    queue_items = list(repeat(queue_item, input_count))

    futures = setup_futures(thread_pool_executor_1, queue_items, download_function, cancel_token=cancel_token)

    assert len(queue_items) == len(futures)


@pytest.mark.parametrize(["input_count"], test_cases_input_count)
def test_output_count_does_not_equal_input_count_if_cancelled(
    queue_item: CollectionFileQueueItem,
    thread_pool_executor_1: ThreadPoolExecutor,
    download_function: DownloadFunction,
    cancel_token: CancellationToken,
    input_count: int,
):
    queue_items = list(repeat(queue_item, input_count))
    cancel_token.cancel()

    futures = setup_futures(thread_pool_executor_1, queue_items, download_function, cancel_token=cancel_token)

    assert len(queue_items) > len(futures)
