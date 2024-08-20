from pathlib import Path

from src.app.core.models.filesystem import FileSystem
from src.app.importer.utils import check_if_exists


def test_returns_false_on_non_existing_file(filesystem: FileSystem):
    file_path = "/var/conftest.py"

    assert check_if_exists(file_path, 0, filesystem=filesystem) is False
    assert check_if_exists(Path(file_path), 0, filesystem=filesystem) is False


def test_returns_false_on_size_not_match(filesystem: FileSystem, google_drive_file_content: str):
    file_path = "/var/conftest.py"
    filesystem.write(file_path, google_drive_file_content)

    assert check_if_exists(file_path, 512, filesystem=filesystem) is False
    assert check_if_exists(Path(file_path), 512, filesystem=filesystem) is False


def test_returns_true(filesystem: FileSystem, google_drive_file_content: str):
    file_path = "/var/conftest.py"
    filesystem.write(file_path, google_drive_file_content)
    expected_size = len(google_drive_file_content)

    assert check_if_exists(file_path, expected_size, filesystem=filesystem) is True
    assert check_if_exists(Path(file_path), expected_size, filesystem=filesystem) is True
