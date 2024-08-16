import asyncio
import queue

from ..log.log_importer import database_worker as log
from ..models import CancellationToken, CollectionFileChange, QueueItem, StatusFlag


async def worker(
    database_queue: queue.Queue,
    status_flag: StatusFlag,
    cancel_token: CancellationToken,
):
    status_flag.value = True
    log.database_worker_started()

    while not cancel_token.cancelled:
        await process_queue_item(database_queue)

    log.database_worker_ended(cancel_token)
    status_flag.value = False


async def process_queue_item(database_queue: queue.Queue) -> int:
    queue_item: QueueItem
    change: CollectionFileChange

    try:
        queue_item, change = database_queue.get(block=False)
    except queue.Empty:
        await asyncio.sleep(0.01)
        return

    await update_queue_item(queue_item, change)

    database_queue.task_done()
    return


async def update_queue_item(queue_item: QueueItem, change: CollectionFileChange):
    await queue_item.update_collection_file(change)
    log.queue_item_update(queue_item.item_no, change)
