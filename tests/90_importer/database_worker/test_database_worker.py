import time
from queue import Queue

import pytest

from src.app.core.models import CancellationToken, StatusFlag
from src.app.core.utils import create_async_thread
from src.app.importer.database_worker import worker


def test_status_flag_set(queue: Queue, status_flag_false: StatusFlag, cancel_token: CancellationToken):
    worker_thread = create_async_thread(worker, args=(queue, status_flag_false, cancel_token, 1))
    worker_thread.start()

    time.sleep(0.1)
    assert status_flag_false.value is True

    queue.put((None, None))
    worker_thread.join()

    assert status_flag_false.value is False


def test_exit_on_cancelled(queue: Queue, status_flag_false: StatusFlag, cancel_token: CancellationToken):
    worker_thread = create_async_thread(worker, args=(queue, status_flag_false, cancel_token, 1))
    worker_thread.start()

    cancel_token.cancel()

    worker_thread.join()
    assert status_flag_false.value is False


test_cases_exit_on_none_put = [pytest.param(i, id=str(i)) for i in range(1, 6)]


@pytest.mark.parametrize(["exit_after_none_count"], test_cases_exit_on_none_put)
def test_exit_on_none_put(queue: Queue, status_flag_false: StatusFlag, cancel_token: CancellationToken, exit_after_none_count: int):
    worker_thread = create_async_thread(worker, args=(queue, status_flag_false, cancel_token, exit_after_none_count))
    worker_thread.start()

    time.sleep(0.1)

    for _ in range(exit_after_none_count):
        assert worker_thread.is_alive()
        queue.put((None, None))

    worker_thread.join()
