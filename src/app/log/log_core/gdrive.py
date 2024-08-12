from contextlib import contextmanager
from pathlib import Path
from typing import Union

from .. import LOGGER_BASE


LOGGER = LOGGER_BASE.getChild("GoogleDriveClient")


def client_creating():
    LOGGER.info("Creating GoogleDriveClient.")


def credentials_json_created(file_path: Union[Path, str]):
    LOGGER.info("Created Service Account Credentials file: %s", file_path)


def credentials_json_exists(file_path: Union[Path, str]):
    LOGGER.info("Using existing Service Account Credentials file: %s", file_path)


@contextmanager
def download_file(file_name: str):
    LOGGER.debug("Downloading file: %s", file_name)
    yield
    LOGGER.debug("Downloaded file: %s", file_name)


def download_file_error(file_name: str, exception: Exception):
    LOGGER.warn("An error occured while downloading file '%s': %s", file_name, exception)


def settings_yaml_exists(file_path: Union[Path, str]):
    LOGGER.info("Using existing Settings file: %s", file_path)


def settings_yaml_created(file_path: Union[Path, str]):
    LOGGER.info("Created Settings file: %s", file_path)
