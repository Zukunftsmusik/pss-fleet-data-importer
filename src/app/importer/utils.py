import logging
from pathlib import Path
from typing import Optional, Union


def check_if_exists(item_no: int, expected_file_size: int, check_path: Union[Path, str], logger: Optional[logging.Logger] = None) -> bool:
    if logger:
        logger.debug("Checking if file no. %i exists at: %s", item_no, check_path)

    if check_path.exists():
        if logger:
            logger.debug("Getting file size of file no. %i.", item_no)

        file_size = check_path.stat().st_size
        if logger:
            logger.debug("File size of file no. %i is: %i", item_no, file_size)
            logger.debug("Checking if file size matches gdrive file for file no. %i.", item_no)

        if file_size == expected_file_size:
            if logger:
                logger.debug("File size of file no. %i matches gdrive file.", item_no)
            return True
        else:
            if logger:
                logger.debug("File size of file no. %i does not match gdrive file (expected %i).", item_no, expected_file_size)
    else:
        if logger:
            logger.debug("File no. %i does not exists at: %s", item_no, check_path)

    return False
