from ...core.models.cancellation_token import CancellationToken
from ...core.models.collection_file_change import CollectionFileChange
from ...models.queue_item import CollectionFileQueueItem
from .importer import LOGGER as LOGGER_IMPORTER
from .importer import worker_ended, worker_started


LOGGER = LOGGER_IMPORTER.getChild("databaseWorker")
WORKER_NAME = "Database"


def database_worker_ended(cancel_token: CancellationToken):
    worker_ended(WORKER_NAME, cancel_token)


def database_worker_started():
    worker_started(WORKER_NAME)


def queue_item_update(queue_item: CollectionFileQueueItem, change: CollectionFileChange):
    LOGGER.debug("Updated queue item no. %i: %s", queue_item.item_no, change)


__all__ = [
    database_worker_ended.__name__,
    database_worker_started.__name__,
    queue_item_update.__name__,
]
