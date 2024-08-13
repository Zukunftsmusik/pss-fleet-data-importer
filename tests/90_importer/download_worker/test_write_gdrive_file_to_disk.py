from pathlib import Path
from typing import Union

import pytest

from src.app.core.models.cancellation_token import CancellationToken, OperationCancelledError
from src.app.core.models.filesystem import FileSystem
from src.app.importer.download_worker import write_gdrive_file_to_disk
from tests.fake_classes import FakeFileSystem


def test_return_none_if_content_is_none(
    filesystem: FileSystem,
    cancel_token: CancellationToken,
    google_drive_file_name: str,
):
    file_path = "/dev/bull/abc.def"
    download_file_path, download_error = write_gdrive_file_to_disk(
        None,
        file_path,
        cancel_token,
        1,
        google_drive_file_name,
        1,
        filesystem=filesystem,
    )

    assert download_file_path is None
    assert download_error is True
    assert filesystem.exists(file_path) is False


def test_return_none_if_content_is_empty(
    filesystem: FileSystem,
    cancel_token: CancellationToken,
    google_drive_file_name: str,
):
    file_path = "/dev/bull/abc.def"
    download_file_path, download_error = write_gdrive_file_to_disk(
        "",
        file_path,
        cancel_token,
        1,
        google_drive_file_name,
        1,
        filesystem=filesystem,
    )

    assert download_file_path is None
    assert download_error is True
    assert filesystem.exists(file_path) is False


test_cases_attemps = [pytest.param(i, id=str(i)) for i in range(1, 6)]


@pytest.mark.parametrize(["max_write_attempts"], test_cases_attemps)
def test_raise_io_error_after_attempts_used(
    filesystem: FileSystem,
    cancel_token: CancellationToken,
    google_drive_file_name: str,
    max_write_attempts: int,
    monkeypatch: pytest.MonkeyPatch,
):
    def mock_write(path: Union[Path, str], content: str, mode: str = "w"):
        raise IOError()

    monkeypatch.setattr(FakeFileSystem, FakeFileSystem.write.__name__, mock_write)

    file_path = "/dev/bull/abc.def"
    with pytest.raises(IOError):
        _ = write_gdrive_file_to_disk(
            "abcdef",
            file_path,
            cancel_token,
            1,
            google_drive_file_name,
            max_write_attempts,
            filesystem=filesystem,
        )


def test_file_written(
    filesystem: FileSystem,
    cancel_token: CancellationToken,
    google_drive_file_name: str,
    google_drive_file_content: str,
):
    file_path = "/dev/bull/abc.def"

    download_file_path, download_error = write_gdrive_file_to_disk(
        google_drive_file_content,
        file_path,
        cancel_token,
        1,
        google_drive_file_name,
        1,
        filesystem=filesystem,
    )

    assert download_file_path == file_path
    assert download_error is False
    assert filesystem.exists(file_path) is True
    assert filesystem.read(file_path) == google_drive_file_content


def test_raises_operation_cancelled_error_if_cancelled(
    filesystem: FileSystem,
    cancel_token: CancellationToken,
    google_drive_file_name: str,
    google_drive_file_content: str,
):
    file_path = "/dev/bull/abc.def"
    cancel_token.cancel()

    with pytest.raises(OperationCancelledError):
        _ = write_gdrive_file_to_disk(
            google_drive_file_content,
            file_path,
            cancel_token,
            1,
            google_drive_file_name,
            1,
            filesystem=filesystem,
        )

    assert filesystem.exists(file_path) is False
