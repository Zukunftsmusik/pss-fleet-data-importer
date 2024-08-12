from pathlib import Path
from typing import Union

from ..log.log_importer import utils as log


def check_if_exists(file_path: Union[Path, str], item_no: int, expected_file_size: int, log_details: bool = False) -> bool:
    if log_details:
        log.check_if_exist_start(item_no, file_path)

    if file_path.exists():
        if log_details:
            log.get_file_size(item_no, file_path)

        file_size = file_path.stat().st_size
        if log_details:
            log.file_sizes(item_no, file_size)
            log.check_if_file_sizes_match(item_no)

        if file_size == expected_file_size:
            if log_details:
                log.file_sizes_match(item_no)
            return True
        else:
            if log_details:
                log.file_sizes_dont_match(item_no, file_size, expected_file_size)
    else:
        if log_details:
            log.does_not_exist(item_no, file_path)

    return False
