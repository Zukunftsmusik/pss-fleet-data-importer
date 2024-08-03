import random
import sys
from asyncio import sleep
from io import StringIO
from time import perf_counter

import pytest

from src.importer.core import utils


ADDITIONAL_WAIT_TIME = 0.1

MIN_WAIT_TIME = 0.5
MAX_WAIT_TIME = 1

MIN_WORKER_COUNT = 1
MAX_WORKER_COUNT = 5

test_cases = [
    # max_workers: int, waiter_count: int, min_wait_time: float, max_wait_time: float
    pytest.param(
        i,
        i + 3,
        MIN_WAIT_TIME,
        MAX_WAIT_TIME,
        id=f"{i}_workers",
    )
    for i in range(MIN_WORKER_COUNT, MAX_WORKER_COUNT + 1)
]
"""max_workers: int, waiter_count: int, min_wait_time: float, max_wait_time: float"""


@pytest.mark.parametrize(["max_workers", "waiter_count", "min_wait_time", "max_wait_time"], test_cases)
def test_run_async_thread_pool_executor(
    max_workers: int, waiter_count: int, min_wait_time: float, max_wait_time: float, capsys: pytest.CaptureFixture
):
    waiters = [(i + 1, min_wait_time + random.random() * (max_wait_time - min_wait_time)) for i in range(waiter_count)]

    sys.stdout = StringIO()

    start = perf_counter()
    executor = utils.run_async_thread_pool_executor(worker, waiters, max_workers)
    executor.shutdown(wait=True)
    end = perf_counter()

    run_time = end - start
    output = sys.stdout.getvalue()
    output_lines = output.split("\n")

    with capsys.disabled():
        sys.stdout = sys.__stdout__
        print()
        print(f"Waiter count: {waiter_count}")
        print(f"Max workers: {max_workers}")
        print(output)
        print(f"Run time: {run_time:.2f} seconds")

    for i in range(max_workers):
        assert "is sleeping" in output_lines[i]


async def worker(worker_id: int, wait_time: float):
    await sleep(ADDITIONAL_WAIT_TIME)
    print(f"Waiter {worker_id} is sleeping for {wait_time:.2f} seconds.")
    await sleep(wait_time)
    print(f"Waiter {worker_id} woke up.")
