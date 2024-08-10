import asyncio
import json
import logging
import queue
from datetime import datetime, timezone

from pss_fleet_data import PssFleetDataClient
from pss_fleet_data.core.exceptions import ApiError, NonUniqueTimestampError

from ..core import utils
from ..log.log_importer import importer as log
from ..models import CancellationToken, CollectionFileChange, CollectionFileQueueItem, StatusFlag


async def worker(
    import_queue: queue.Queue,
    database_queue: queue.Queue,
    fleet_data_client: PssFleetDataClient,
    status_flag: StatusFlag,
    parent_logger: logging.Logger,
    cancel_token: CancellationToken,
    exit_after_none_count: int,
    keep_downloaded_files: bool = False,
):
    status_flag.value = True
    parent_logger.info("Import worker started...")
    logger = parent_logger.parent.getChild("importWorker")

    queue_item: CollectionFileQueueItem = None
    none_count = 0

    while not cancel_token.cancelled:
        try:
            queue_item = import_queue.get(block=False)
        except queue.Empty:
            await asyncio.sleep(0.1)
            continue

        if queue_item is None:
            none_count += 1

            if none_count == exit_after_none_count:
                break
            else:
                continue

        if await skip_file_import_on_error(logger, queue_item.item_no, queue_item):
            logger.error("Could not import file no. %i: %s", queue_item.item_no, queue_item.gdrive_file_name)
            import_queue.task_done()
            continue

        logger.debug("Importing file no. %i: %s", queue_item.item_no, queue_item.download_file_path)

        try:
            imported_at = await import_file(fleet_data_client, queue_item, logger)
        except ApiError as exc:
            logger.error("Could not import file no. %i: %s", queue_item.item_no, queue_item.gdrive_file_name)
            logger.error(exc, exc_info=True)
            continue

        database_queue.put((queue_item, CollectionFileChange(imported_at=imported_at)))

        if not keep_downloaded_files:
            queue_item.download_file_path.unlink(missing_ok=True)

        import_queue.task_done()

    log.worker_ended(parent_logger, "Import worker", cancel_token)

    database_queue.put((None, None))
    status_flag.value = False


async def import_file(fleet_data_client: PssFleetDataClient, queue_item: CollectionFileQueueItem, logger: logging.Logger) -> datetime:
    try:
        collection_metadata = await fleet_data_client.upload_collection(queue_item.download_file_path)
        imported_at = utils.remove_timezone(datetime.now(tz=timezone.utc))
        logger.info(
            "Imported file no. %i (Collection ID: %i): %s", queue_item.item_no, collection_metadata.collection_id, queue_item.download_file_path
        )
    except NonUniqueTimestampError:
        imported_at = utils.remove_timezone(datetime.now(tz=timezone.utc))
        collection_metadata = await fleet_data_client.get_most_recent_collection_metadata_by_timestamp(queue_item.collection_file.timestamp)
        logger.info(
            "Skipped file no. %i (Collection already exists with ID: %i): %s",
            queue_item.item_no,
            collection_metadata.collection_id,
            queue_item.download_file_path,
        )

    return imported_at


async def skip_file_import_on_error(logger: logging.Logger, file_no: int, queue_item: CollectionFileQueueItem) -> bool:
    if queue_item.cancel_token.cancelled:
        return True

    if queue_item.error_while_downloading:
        logger.warn("Error while downloading. Skipping file no. %i: %s", file_no, queue_item.gdrive_file_name)
        return True

    with open(queue_item.download_file_path, "r") as fp:
        contents = json.load(fp)
        if not contents:
            logger.warn("File contains empty json. Skipping file no. %i: %s", file_no, queue_item.download_file_path)
            return True

    return False
