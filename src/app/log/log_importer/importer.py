import logging
from datetime import datetime
from typing import Optional

from pydrive2.files import ApiRequestError

from ...core.models.cancellation_token import CancellationToken
from ...models.queue_item import CollectionFileQueueItem
from . import LOGGER_IMPORTER as LOGGER


def bulk_import_finish(
    logger: logging.Logger, queue_items: list[CollectionFileQueueItem], modified_after: Optional[datetime], modified_before: Optional[datetime]
):
    total_item_count = len(queue_items)
    downloaded_item_count = len([queue_item for queue_item in queue_items if queue_item.collection_file.downloaded_at])
    imported_item_count = len([queue_item for queue_item in queue_items if queue_item.collection_file.imported_at])

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


def bulk_import_start(logger: logging.Logger, modified_after: Optional[datetime], modified_before: Optional[datetime]):
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


def downloads_imports(logger: logging.Logger, queue_items: list[CollectionFileQueueItem]):
    download_count = len([_ for _ in queue_items if _.collection_file.downloaded_at is None])
    import_count = len([_ for _ in queue_items if _.collection_file.imported_at is None])
    LOGGER.info(f"Downloading {download_count} Collection files and importing {import_count} Collection files.")


def gdrive_file_list_params(logger: logging.Logger, modified_after: Optional[datetime], modified_before: Optional[datetime]):
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


def download_error(logger: logging.Logger, item_no: int, gdrive_file_name: str, log_stack_trace: bool, exc: ApiRequestError, sleep_for: int):
    msg = f"An error occured while downloading the file no. {item_no} '{gdrive_file_name}' from Drive. Retrying in about {sleep_for} seconds."
    if log_stack_trace:
        LOGGER.error(msg, exc_info=exc)
    else:
        LOGGER.error("%s:  %s", msg, type(exc))


def worker_ended(logger: logging.Logger, worker_name: str, cancel_token: CancellationToken = None):
    if cancel_token and cancel_token.cancelled:
        LOGGER.info("%s cancelled.", worker_name.strip())
    else:
        LOGGER.info("%s finished.", worker_name.strip())


def gdrive_file_download(logger: logging.Logger, attempt: int, item_no: int, gdrive_file_name: str):
    if attempt > 0:
        LOGGER.warn("Attempt %i at downloading file no. %i: %s", attempt + 1, item_no, gdrive_file_name)
    else:
        LOGGER.debug("Downloading file no. %i: %s", item_no, gdrive_file_name)
