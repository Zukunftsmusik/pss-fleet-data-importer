import asyncio
import queue

from ..log.log_importer import database_worker as log
from ..models import CancellationToken, CollectionFileChange, CollectionFileQueueItem, StatusFlag


async def worker(
    database_queue: queue.Queue,
    status_flag: StatusFlag,
    cancel_token: CancellationToken,
    exit_after_none_count: int,
):
    status_flag.value = True
    log.database_worker_started()

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

        await update_queue_item(change)

        database_queue.task_done()

    log.database_worker_ended(cancel_token)
    status_flag.value = False


async def update_queue_item(queue_item: CollectionFileQueueItem, change: CollectionFileChange):
    await queue_item.update_collection_file(change)
    log.queue_item_update(queue_item.item_no, change)
