from pss_fleet_data import PssFleetDataClient
from pss_fleet_data.core.exceptions import ApiError, NonUniqueTimestampError

from ..core.models.filesystem import FileSystem
from ..log.log_importer import import_worker as log
from ..models import QueueItem


async def process_queue_item(
    queue_item: QueueItem,
    fleet_data_client: PssFleetDataClient,
    keep_downloaded_files: bool,
    filesystem: FileSystem = FileSystem(),
):
    if skip_file_import_on_error(queue_item, filesystem=filesystem):
        log.skip_file_error(queue_item.item_no, queue_item.gdrive_file.name)
    else:
        log.import_start(queue_item.item_no, queue_item.target_file_path)
        await do_import(fleet_data_client, queue_item, keep_downloaded_files, filesystem=filesystem)


async def do_import(
    fleet_data_client: PssFleetDataClient,
    queue_item: QueueItem,
    keep_downloaded_files: bool,
    filesystem: FileSystem = FileSystem(),
):
    try:
        await import_file(fleet_data_client, queue_item)
    except ApiError as exc:
        log.file_import_api_error(queue_item.item_no, queue_item.gdrive_file.name, exc)
        queue_item.status.import_error.value = True
    else:
        queue_item.status.imported.value = True

        if not keep_downloaded_files:
            filesystem.delete(queue_item.target_file_path, missing_ok=True)


async def import_file(fleet_data_client: PssFleetDataClient, queue_item: QueueItem):
    try:
        collection_metadata = await fleet_data_client.upload_collection(queue_item.target_file_path)
        log.file_import_completed(queue_item.item_no, queue_item.target_file_path, collection_metadata.collection_id)
    except NonUniqueTimestampError:
        log.file_import_skipped(queue_item.item_no, queue_item.target_file_path)


def skip_file_import_on_error(queue_item: QueueItem, filesystem: FileSystem = FileSystem()) -> bool:
    if queue_item.status.cancel_token.cancelled:
        return True

    if queue_item.status.download_error:
        log.skip_file_import_download_error(queue_item.item_no, queue_item.gdrive_file.name)
        return True

    contents = filesystem.load_json(queue_item.target_file_path)
    if not contents:
        log.skip_file_import_empty_json(queue_item.item_no, queue_item.target_file_path)
        return True

    return False
