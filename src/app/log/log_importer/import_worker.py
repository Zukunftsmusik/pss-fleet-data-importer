from pathlib import Path
from typing import Union

from ...core.models.cancellation_token import CancellationToken
from .importer import LOGGER as LOGGER_IMPORTER
from .importer import worker_ended, worker_started


LOGGER = LOGGER_IMPORTER.getChild("importWorker")
WORKER_NAME = "Import"


def file_import_api_error(item_no: int, gdrive_file_name: str, exception: Exception):
    LOGGER.error("Could not import file no. %i: %s", item_no, gdrive_file_name)
    LOGGER.error(exception, exc_info=True)


def file_import_completed(item_no: int, file_path: Union[Path, str], collection_id: int):
    LOGGER.info("Imported file no. %i (Collection ID: %i): %s", item_no, collection_id, file_path)


def file_import_skipped(item_no: int, file_path: Union[Path, str], collection_id: int):
    LOGGER.info("Skipped file no. %i (Collection already exists with ID: %i): %s", item_no, collection_id, file_path)


def import_start(file_no: int, file_path: Union[Path, str]):
    LOGGER.debug("Importing file no. %i: %s", file_no, file_path)


def import_worker_ended(cancel_token: CancellationToken):
    worker_ended(WORKER_NAME, cancel_token)


def import_worker_started():
    worker_started(WORKER_NAME)


def skip_file_error(file_no: int, gdrive_file_name: str):
    LOGGER.error("Could not import file no. %i: %s", file_no, gdrive_file_name)


def skip_file_import_download_error(file_no: int, gdrive_file_name: str):
    LOGGER.debug("Error while downloading. Skipping file no. %i: %s", file_no, gdrive_file_name)


def skip_file_import_empty_json(file_no: int, file_path: Union[Path, str]):
    LOGGER.debug("File contains empty json. Skipping file no. %i: %s", file_no, file_path)


__all__ = [
    file_import_api_error.__name__,
    file_import_completed.__name__,
    file_import_skipped.__name__,
    import_start.__name__,
    import_worker_ended.__name__,
    import_worker_started.__name__,
    skip_file_error.__name__,
    skip_file_import_download_error.__name__,
    skip_file_import_empty_json.__name__,
]
