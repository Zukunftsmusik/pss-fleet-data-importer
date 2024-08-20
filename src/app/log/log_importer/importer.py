from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

from ...core.models.cancellation_token import CancellationToken
from ...core.models.collection_file_change import CollectionFileChange
from ...models.queue_item import QueueItem
from .. import LOGGER_BASE


LOGGER = LOGGER_BASE.getChild("Importer")


def bulk_import_finish(queue_items: list[QueueItem], modified_after: Optional[datetime], modified_before: Optional[datetime]):
    total_item_count = len(queue_items)
    downloaded_item_count = len([queue_item for queue_item in queue_items if queue_item.status.downloaded])
    imported_item_count = len([queue_item for queue_item in queue_items if queue_item.status.imported])

    base_message = f"Finished bulk import. Downloaded {downloaded_item_count}, imported {imported_item_count} out of {total_item_count} files"

    if modified_after:
        if modified_before:
            LOGGER.info("%s modified after %s & modified before %s.", base_message, modified_after.isoformat(), modified_before.isoformat())
        else:
            LOGGER.info("%s modified after %s.", base_message, modified_after.isoformat())
    else:
        if modified_before:
            LOGGER.info("%s modified before %s.", base_message, modified_before.isoformat())
        else:
            LOGGER.info("%s.", base_message)


def bulk_import_finish_time(item_count: int, start: datetime, end: datetime):
    LOGGER.info("### Finished bulk import of %i files at: %s (after: %s)", item_count, end.isoformat(), str(end - start))
    print(f"### Finished bulk import of {item_count} files at: {end.isoformat()} (after: {end - start})")


def bulk_import_start(modified_after: Optional[datetime], modified_before: Optional[datetime]):
    if modified_after:
        if modified_before:
            LOGGER.info(
                "Starting bulk import of files modified after %s & modified before %s.", modified_after.isoformat(), modified_before.isoformat()
            )
        else:
            LOGGER.info("Starting bulk import of files modified after %s.", modified_after.isoformat())
    else:
        if modified_before:
            LOGGER.info("Starting bulk import of files modified before %s.", modified_before.isoformat())
        else:
            LOGGER.info("Starting bulk import.")


def bulk_import_start_time(start: datetime):
    LOGGER.info("### Starting bulk import at: %s", start.isoformat())
    print(f"### Starting bulk import at: {start.isoformat()}")


def database_entries_create():
    LOGGER.debug("Creating database entries.")


def download_folder_create(folder_path: Union[Path, str]):
    LOGGER.debug("Ensuring that download path '%s' exists.", folder_path)


@contextmanager
def download_gdrive_file_list_duration():
    from time import perf_counter

    start = perf_counter()

    yield

    end = perf_counter()
    LOGGER.debug("%s took %.2f seconds", "Downloading file list", end - start)


def download_gdrive_file_list_length(gdrive_files_count: int):
    if gdrive_files_count == 0:
        LOGGER.info("No new files found to be imported.")
    else:
        LOGGER.info("Found %i new gdrive files to be imported.", gdrive_files_count)


def download_gdrive_file_list_start():
    LOGGER.debug("Downloading Google Drive file list.")


def download_gdrive_file_list_params(modified_after: Optional[datetime], modified_before: Optional[datetime]):
    if modified_after or modified_before:
        if modified_after:
            if modified_before:
                LOGGER.info(
                    "Retrieving gdrive files modified after: %s and modified before: %s", modified_after.isoformat(), modified_before.isoformat()
                )
            else:
                LOGGER.info("Retrieving gdrive files modified after: %s", modified_after.isoformat())
        else:
            LOGGER.info("Retrieving gdrive files modified before: %s", modified_before.isoformat())
    else:
        LOGGER.info("Retrieving all gdrive files.")


def downloads_imports_count(queue_items: list[QueueItem]):
    download_count = len([_ for _ in queue_items if not _.status.downloaded])
    import_count = len([_ for _ in queue_items if not _.status.imported])
    LOGGER.info(f"Downloading {download_count} Collection files and importing {import_count} Collection files.")


def queue_item_update(item_no: int, change: CollectionFileChange):
    LOGGER.debug("Updated queue item no. %i: %s", item_no, change)


def queue_items_create():
    LOGGER.debug("Creating queue items.")


def wait_for_import(duration: float, until: datetime):
    LOGGER.info("Waiting for %.2f seconds until next import run at %s.", duration, until.isoformat())


def worker_ended(worker_name: str, cancel_token: CancellationToken = None):
    if cancel_token and cancel_token.cancelled:
        LOGGER.info("%s worker cancelled.", worker_name.strip())
    else:
        LOGGER.info("%s worker finished.", worker_name.strip())


def worker_started(worker_name: str):
    LOGGER.info("%s worker started...", worker_name)


def workers_cancel():
    LOGGER.warn("Cancelling workers.")
