import asyncio
import logging
import queue

from ..database import Database
from ..models.cancellation_token import CancellationToken
from ..models.collection_file_change import CollectionFileChange
from ..models.queue_item import CollectionFileQueueItem
from ..models.status import StatusFlag
from . import log


async def worker(
    database: Database,
    database_queue: queue.Queue,
    parent_logger: logging.Logger,
    status_flag: StatusFlag,
    cancel_token: CancellationToken,
    exit_after_none_count: int,
):
    status_flag.value = True
    parent_logger.info("Database worker started...")
    logger = parent_logger.parent.getChild("databaseWorker")

    queue_item: CollectionFileQueueItem
    change: CollectionFileChange
    none_count = 0

    while not cancel_token.cancelled:
        try:
            queue_item, change = database_queue.get(block=False)
        except queue.Empty:
            await asyncio.sleep(0.1)
            continue

        if queue_item is None and change is None:
            none_count += 1

            if none_count == exit_after_none_count:
                break
            else:
                continue

        await queue_item.update_collection_file(database, change)
        log.queue_item_update(logger, queue_item, change)

        database_queue.task_done()

    if cancel_token.cancelled:
        parent_logger.info("Database worker cancelled.")
    else:
        parent_logger.info("Database worker finished.")
    status_flag.value = False
