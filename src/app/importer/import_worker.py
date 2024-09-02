import asyncio

from pss_fleet_data import PssFleetDataClient
from pss_fleet_data.core.exceptions import ApiError, ConflictError, NonUniqueTimestampError

from ..core import utils
from ..core.models.filesystem import FileSystem
from ..log.log_importer import import_worker as log
from ..models import QueueItem


async def process_queue_item(
    queue_item: QueueItem,
    fleet_data_client: PssFleetDataClient,
    keep_downloaded_files: bool,
    update_existing_collections: bool = False,
    import_attempts: int = 2,
    filesystem: FileSystem = FileSystem(),
):
    if skip_file_import_on_error(queue_item, filesystem=filesystem):
        log.skip_file_error(queue_item.item_no, queue_item.gdrive_file.name)
    else:
        log.import_start(queue_item.item_no, queue_item.target_file_path)
        await do_import(
            fleet_data_client,
            queue_item,
            keep_downloaded_files,
            update_existing_collections=update_existing_collections,
            import_attempts=import_attempts,
            filesystem=filesystem,
        )


async def do_import(
    fleet_data_client: PssFleetDataClient,
    queue_item: QueueItem,
    keep_downloaded_files: bool,
    update_existing_collections: bool = False,
    import_attempts: int = 2,
    filesystem: FileSystem = FileSystem(),
):
    collection_exists = False

    try:
        await upload_collection(fleet_data_client, queue_item, import_attempts=import_attempts, reraise_non_unique_timestamp_error=True)
    except NonUniqueTimestampError:
        collection_exists = True
    except ApiError as exc:
        log.file_import_api_error(queue_item.item_no, queue_item.gdrive_file.name, exc)
        queue_item.status.import_error.value = True
    else:
        collection_exists = True

    if collection_exists:
        if update_existing_collections:
            try:
                await update_collection(fleet_data_client, queue_item, import_attempts=import_attempts)
            except ApiError as exc:
                log.file_import_api_error(queue_item.item_no, queue_item.gdrive_file.name, exc)
                queue_item.status.import_error.value = True
            else:
                queue_item.status.imported.value = True
        else:
            queue_item.status.imported.value = True

    if queue_item.status.imported and not keep_downloaded_files:
        filesystem.delete(queue_item.target_file_path, missing_ok=True)


async def upload_collection(
    fleet_data_client: PssFleetDataClient,
    queue_item: QueueItem,
    reraise_non_unique_timestamp_error: bool = False,
    import_attempts: int = 2,
):
    import_error: Exception = None

    for attempt in range(import_attempts):
        try:
            collection_metadata = await fleet_data_client.upload_collection(queue_item.target_file_path)
        except NonUniqueTimestampError as exc:
            if reraise_non_unique_timestamp_error:
                raise exc
            log.collection_upload_skipped(queue_item.item_no, queue_item.target_file_path)
            return
        except Exception as exc:
            import_error = exc
            log.file_import_error(queue_item.item_no, queue_item.target_file_path, exc)
        else:
            log.file_import_completed(queue_item.item_no, queue_item.target_file_path, collection_metadata.collection_id)
            return

        await asyncio.sleep(2 ^ attempt)

    raise import_error


async def update_collection(
    fleet_data_client: PssFleetDataClient,
    queue_item: QueueItem,
    import_attempts: int = 2,
):
    import_error: Exception = None

    timestamp = utils.extract_timestamp_from_gdrive_file_name(queue_item.gdrive_file.name)
    existing_collection_metadata = await fleet_data_client.get_most_recent_collection_metadata_by_timestamp(timestamp)

    for attempt in range(import_attempts):
        try:
            collection_metadata = await fleet_data_client.update_collection(existing_collection_metadata.collection_id, queue_item.target_file_path)
        except ConflictError:
            log.collection_update_skipped(queue_item.item_no, queue_item.target_file_path)
            return
        except Exception as exc:
            import_error = exc
            log.file_import_error(queue_item.item_no, queue_item.target_file_path, exc)
        else:
            log.file_import_completed(queue_item.item_no, queue_item.target_file_path, collection_metadata.collection_id)
            return

        await asyncio.sleep(2 ^ attempt)

    raise import_error


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
