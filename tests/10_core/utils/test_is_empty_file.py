from src.app.core.models.filesystem import FileSystem
from src.app.core.utils import is_empty_file


def test_return_true_on_empty_file(filesystem: FileSystem):
    file_path = "/var/conftest.py"
    filesystem.write(file_path, "")

    assert is_empty_file(file_path, filesystem=filesystem) is True


def test_return_false_on_non_empty_file(filesystem: FileSystem):
    file_path = "/var/conftest.py"
    filesystem.write(file_path, "abc")

    assert is_empty_file(file_path, filesystem=filesystem) is False
