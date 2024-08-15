from datetime import datetime
from queue import Queue

import pytest

from src.app.core.models.collection_file_change import CollectionFileChange
from src.app.importer.database_worker import process_queue_item
from src.app.models.queue_item import QueueItem


async def test_returns_none_count_on_empty_queue(queue: Queue):
    none_count = 0
    none_count = await process_queue_item(queue, none_count)

    assert none_count == 0


async def test_returns_none_count_plus_1_on_none_in_queue(queue: Queue):
    queue.put((None, None))
    none_count = 0
    none_count = await process_queue_item(queue, none_count)

    assert none_count == 1


@pytest.mark.usefixtures("patch_crud_save_collection")
async def test_updates_queue_item(queue: Queue, queue_item: QueueItem):
    downloaded_at = datetime(2024, 8, 1)
    change = CollectionFileChange(downloaded_at=downloaded_at)
    queue.put((queue_item, change))
    none_count = 0

    assert queue.unfinished_tasks > 0

    none_count = await process_queue_item(queue, none_count)

    assert none_count == 0
    assert queue_item.collection_file.downloaded_at == downloaded_at
    assert queue.unfinished_tasks == 0


@pytest.mark.usefixtures("patch_crud_save_collection")
async def test_notifies_task_done(queue: Queue, queue_item: QueueItem):
    downloaded_at = datetime(2024, 8, 1)
    change = CollectionFileChange(downloaded_at=downloaded_at)

    # A proper queue task
    queue.put((queue_item, change))
    assert queue.unfinished_tasks > 0

    _ = await process_queue_item(queue, 0)

    assert queue.unfinished_tasks == 0

    # None task
    queue.put((None, None))
    assert queue.unfinished_tasks > 0

    _ = await process_queue_item(queue, 0)

    assert queue.unfinished_tasks == 0
