from pathlib import Path
from typing import Optional, Union

from ..core.models.filesystem import FileSystem
from ..log.log_importer import utils as log


def check_if_exists(
    file_path: Union[Path, str],
    expected_file_size: int,
    filesystem: Optional[FileSystem] = None,
) -> bool:
    filesystem = filesystem or FileSystem()

    file_path = Path(file_path)
    if filesystem.exists(file_path):

        file_size = filesystem.get_size(file_path)

        if file_size == expected_file_size:
            return True

    return False
