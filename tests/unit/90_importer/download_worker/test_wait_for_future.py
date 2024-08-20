from concurrent.futures import Future

from src.app.core.models.cancellation_token import CancellationToken
from src.app.importer.download_worker import wait_for_future
from src.app.models.queue_item import QueueItem


def test_cancel_future_if_not_done_and_cancel_token_cancelled(queue_item: QueueItem, cancel_token: CancellationToken):
    future = Future()
    cancel_token.cancel()

    wait_for_future(future, queue_item, None, cancel_token)

    assert future.cancelled() is True


def test_dont_cancel_future_if_done_and_cancel_token_cancelled(queue_item: QueueItem, cancel_token: CancellationToken):
    future = Future()
    future.set_result(None)

    cancel_token.cancel()

    wait_for_future(future, queue_item, None, cancel_token)

    assert future.cancelled() is False
