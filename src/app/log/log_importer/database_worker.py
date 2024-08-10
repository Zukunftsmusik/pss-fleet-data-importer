import logging

from ...core.models.collection_file_change import CollectionFileChange
from ...models.queue_item import CollectionFileQueueItem
from . import LOGGER_IMPORTER


LOGGER = LOGGER_IMPORTER.getChild("databaseWorker")


def queue_item_update(logger: logging.Logger, queue_item: CollectionFileQueueItem, change: CollectionFileChange):
    LOGGER.debug("Updated queue item no. %i: %s", queue_item.item_no, change)
