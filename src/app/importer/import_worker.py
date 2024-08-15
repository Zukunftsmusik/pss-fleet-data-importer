import asyncio
import queue
from datetime import datetime, timezone

from pss_fleet_data import PssFleetDataClient
from pss_fleet_data.core.exceptions import ApiError, NonUniqueTimestampError

from ..core import utils
from ..core.models.filesystem import FileSystem
from ..log.log_importer import import_worker as log
from ..models import CancellationToken, CollectionFileChange, QueueItem, StatusFlag


async def worker(
    import_queue: queue.Queue,
    database_queue: queue.Queue,
    fleet_data_client: PssFleetDataClient,
    status_flag: StatusFlag,
    cancel_token: CancellationToken,
    exit_after_none_count: int,
    keep_downloaded_files: bool = False,
    filesystem: FileSystem = FileSystem(),
):
    status_flag.value = True
    log.import_worker_started()

    none_count = 0

    while not cancel_token.cancelled and not none_count >= exit_after_none_count:
        none_count = await process_queue_item(
            fleet_data_client, import_queue, database_queue, none_count, keep_downloaded_files, filesystem=filesystem
        )

    log.import_worker_ended(cancel_token)
    database_queue.put((None, None))

    status_flag.value = False


async def process_queue_item(
    fleet_data_client: PssFleetDataClient,
    import_queue: queue.Queue,
    database_queue: queue.Queue,
    none_count: int,
    keep_downloaded_files: bool,
    filesystem: FileSystem = FileSystem(),
) -> int:
    try:
        queue_item: QueueItem = import_queue.get(block=False)
    except queue.Empty:
        await asyncio.sleep(0.1)
        return none_count

    if queue_item is None:
        none_count += 1
    elif skip_file_import_on_error(queue_item, filesystem=filesystem):
        log.skip_file_error(queue_item.item_no, queue_item.gdrive_file.name)
    else:
        log.import_start(queue_item.item_no, queue_item.target_file_path)
        await do_import(fleet_data_client, queue_item, database_queue, keep_downloaded_files, filesystem=filesystem)

    import_queue.task_done()
    return none_count


async def do_import(
    fleet_data_client: PssFleetDataClient,
    queue_item: QueueItem,
    database_queue: queue.Queue,
    keep_downloaded_files: bool,
    filesystem: FileSystem = FileSystem(),
):
    try:
        imported_at = await import_file(fleet_data_client, queue_item)
    except ApiError as exc:
        log.file_import_api_error(queue_item.item_no, queue_item.gdrive_file.name, exc)
    else:
        database_queue.put((queue_item, CollectionFileChange(imported_at=imported_at)))

        if not keep_downloaded_files:
            filesystem.delete(queue_item.target_file_path, missing_ok=True)


async def import_file(fleet_data_client: PssFleetDataClient, queue_item: QueueItem) -> datetime:
    try:
        collection_metadata = await fleet_data_client.upload_collection(queue_item.target_file_path)
        imported_at = utils.remove_timezone(datetime.now(tz=timezone.utc))
        log.file_import_completed(queue_item.item_no, queue_item.target_file_path, collection_metadata.collection_id)
    except NonUniqueTimestampError:
        imported_at = utils.remove_timezone(datetime.now(tz=timezone.utc))
        collection_metadata = await fleet_data_client.get_most_recent_collection_metadata_by_timestamp(queue_item.collection_file.timestamp)
        log.file_import_skipped(queue_item.item_no, queue_item.target_file_path, collection_metadata.collection_id)

    return imported_at


def skip_file_import_on_error(queue_item: QueueItem, filesystem: FileSystem = FileSystem()) -> bool:
    if queue_item.cancel_token.cancelled:
        return True

    if queue_item.error_while_downloading:
        log.skip_file_import_download_error(queue_item.item_no, queue_item.gdrive_file.name)
        return True

    contents = filesystem.load_json(queue_item.target_file_path)
    if not contents:
        log.skip_file_import_empty_json(queue_item.item_no, queue_item.target_file_path)
        return True

    return False
