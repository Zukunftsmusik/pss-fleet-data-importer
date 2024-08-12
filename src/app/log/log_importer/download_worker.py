from pathlib import Path
from typing import Union

from ...core.models.cancellation_token import CancellationToken
from .importer import LOGGER as LOGGER_IMPORTER
from .importer import worker_ended, worker_started


LOGGER = LOGGER_IMPORTER.getChild("downloadWorker")
WORKER_NAME = "Download"


def download_completed(item_no: int, gdrive_file_name: str):
    LOGGER.debug("File no. %i downloaded: %s", item_no, gdrive_file_name)


def download_error(item_no: int, gdrive_file_name: str, log_stack_trace: bool, exc: Exception, sleep_for: int):
    msg = f"An error occured while downloading the file no. {item_no} '{gdrive_file_name}' from Drive. Retrying in about {sleep_for} seconds."
    if log_stack_trace:
        LOGGER.error(msg, exc_info=exc)
    else:
        LOGGER.error("%s:  %s", msg, type(exc))


def download_gdrive_file(attempt: int, item_no: int, gdrive_file_name: str):
    if attempt > 0:
        LOGGER.warn("Attempt %i at downloading file no. %i: %s", attempt + 1, item_no, gdrive_file_name)
    else:
        LOGGER.debug("Downloading file no. %i: %s", item_no, gdrive_file_name)


def download_worker_ended(cancel_token: CancellationToken):
    worker_ended(WORKER_NAME, cancel_token)


def download_worker_started():
    worker_started(WORKER_NAME)


def file_delete(item_no: int):
    LOGGER.debug("Making sure that file no. %i does not exist.", item_no)


def file_exists(item_no: int, file_path: Union[Path, str]):
    LOGGER.debug("File no. %i already exists: %s", item_no, file_path)


def future_error(future_no: int, exception: Exception):
    LOGGER.warn("Future no. %i raised an error: %s", future_no, exception)


def future_timeout(future_no: int):
    LOGGER.warn("Future no. %i timed out.", future_no)


def thread_pool_cancel():
    LOGGER.debug("Shutting down thread pool, waiting for running downloads to complete.")


def thread_pool_setup(worker_count: int):
    LOGGER.debug("Setting up thread pool for downloads with %i workers.", worker_count)


def wait_for_futures():
    LOGGER.debug("Waiting for download futures to finish.")


def write_file_to_disk(item_no: int, file_path: Union[Path, str]):
    LOGGER.debug("File no. %i written to disk: %s", item_no, file_path)


__all__ = [
    download_completed.__name__,
    download_error.__name__,
    download_gdrive_file.__name__,
    download_worker_ended.__name__,
    download_worker_started.__name__,
    file_delete.__name__,
    file_exists.__name__,
    future_error.__name__,
    future_timeout.__name__,
    thread_pool_cancel.__name__,
    thread_pool_setup.__name__,
    wait_for_futures.__name__,
    write_file_to_disk.__name__,
]
