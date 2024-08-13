from pathlib import Path
from typing import Union

from .importer import LOGGER as LOGGER_IMPORTER


LOGGER = LOGGER_IMPORTER.getChild("utils")


def check_if_exist_start(item_no: int, file_path: Union[Path, str]):
    LOGGER.debug("Checking if file no. %i exists at: %s", item_no, file_path)


def check_if_file_sizes_match(item_no: int):
    LOGGER.debug("Checking if file size matches gdrive file for file no. %i.", item_no)


def does_not_exist(item_no: int, file_path: Union[Path, str]):
    LOGGER.debug("File no. %i does not exists at: %s", item_no, file_path)


def file_size(item_no: int, file_size: int):
    LOGGER.debug("File size of file no. %i is: %i", item_no, file_size)


def file_sizes_dont_match(item_no: int, file_size: int, expected_file_size: int):
    LOGGER.debug("File size of file no. %i does not match gdrive file (expected: %i, got: %i).", item_no, expected_file_size, file_size)


def file_sizes_match(item_no: int):
    LOGGER.debug("File size of file no. %i matches gdrive file.", item_no)


def get_file_size(item_no: int):
    LOGGER.debug("Getting file size of file no. %i.", item_no)
