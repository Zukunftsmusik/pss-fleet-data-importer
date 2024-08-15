import logging
from typing import Optional

import pytest
from importer_test_cases import test_cases_raised_error_caught

from src.app.core.models.filesystem import FileSystem
from src.app.importer import download_worker
from src.app.importer.download_worker import download_gdrive_file
from src.app.importer.exceptions import DownloadFailedError
from src.app.models import QueueItem


def test_logs_if_file_already_exists(queue_item: QueueItem, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch):
    def mock_file_already_downloaded_returns_true(queue_item: QueueItem, filesystem: FileSystem = FileSystem()):
        return True

    monkeypatch.setattr(download_worker, download_worker.file_already_downloaded.__name__, mock_file_already_downloaded_returns_true)

    with caplog.at_level(logging.DEBUG):
        download_gdrive_file(queue_item, None, False)

    assert "already exists" in caplog.text
    assert len(caplog.records) == 1


@pytest.mark.usefixtures("patch_file_already_exists_returns_false")
@pytest.mark.parametrize(["exception_type"], test_cases_raised_error_caught)
def test_raise_download_error_on_google_api_errors(
    queue_item: QueueItem,
    google_api_errors: dict[type[Exception], Exception],
    exception_type: type[Exception],
    monkeypatch: pytest.MonkeyPatch,
):
    def mock_download_gdrive_file_contents_raises_google_api_error(
        gdrive_file,
        gdrive_client,
        cancel_token,
        item_no,
        max_download_attempts,
        log_stack_trace,
    ):
        raise google_api_errors[exception_type]

    monkeypatch.setattr(
        download_worker,
        download_worker.download_gdrive_file_contents.__name__,
        mock_download_gdrive_file_contents_raises_google_api_error,
    )

    with pytest.raises(DownloadFailedError) as exc_info:
        download_gdrive_file(queue_item, None, False)

    exc = exc_info.value
    assert isinstance(exc.inner_exception, exception_type)
    assert exc.file_name == queue_item.gdrive_file.name


test_cases_empty_file_contents = [
    # file_contents
    pytest.param(None, id="none"),
    pytest.param("", id="empty"),
]
"""file_contents: str"""


@pytest.mark.usefixtures("patch_file_already_exists_returns_false")
@pytest.mark.parametrize(["file_contents"], test_cases_empty_file_contents)
def test_raise_download_error_on_downloaded_file_contents_empty_or_none(
    queue_item: QueueItem, file_contents: Optional[str], monkeypatch: pytest.MonkeyPatch
):
    def mock_download_gdrive_file_contents(
        gdrive_file,
        gdrive_client,
        cancel_token,
        item_no,
        max_download_attempts,
        log_stack_trace,
    ):
        return file_contents

    monkeypatch.setattr(download_worker, download_worker.download_gdrive_file_contents.__name__, mock_download_gdrive_file_contents)

    with pytest.raises(DownloadFailedError) as exc_info:
        download_gdrive_file(queue_item, None, False)

    exc = exc_info.value
    assert exc.inner_exception is None
    assert exc.file_name == queue_item.gdrive_file.name


@pytest.mark.usefixtures("patch_file_already_exists_returns_false", "patch_download_gdrive_file_contents_return_something")
def test_raise_download_error_on_io_error(queue_item: QueueItem, monkeypatch: pytest.MonkeyPatch):
    def mock_write_gdrive_file_to_disk_raises_io_error(
        gdrive_file,
        gdrive_client,
        cancel_token,
        item_no,
        max_download_attempts,
        log_stack_trace,
    ):
        raise IOError()

    monkeypatch.setattr(download_worker, download_worker.write_gdrive_file_to_disk.__name__, mock_write_gdrive_file_to_disk_raises_io_error)

    with pytest.raises(DownloadFailedError) as exc_info:
        download_gdrive_file(queue_item, None, False)

    exc = exc_info.value
    assert isinstance(exc.inner_exception, IOError)
    assert exc.file_name == queue_item.gdrive_file.name


@pytest.mark.usefixtures("patch_file_already_exists_returns_false", "patch_download_gdrive_file_contents_return_something")
def test_log_file_downloaded_on_success(queue_item: QueueItem, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch):
    def mock_write_gdrive_file_to_disk_does_nothing(
        gdrive_file,
        gdrive_client,
        cancel_token,
        item_no,
        max_download_attempts,
        log_stack_trace,
    ):
        pass

    monkeypatch.setattr(download_worker, download_worker.write_gdrive_file_to_disk.__name__, mock_write_gdrive_file_to_disk_does_nothing)

    with caplog.at_level(logging.DEBUG):
        download_gdrive_file(queue_item, None, False)

    assert "downloaded" in caplog.text.lower()
    assert len(caplog.records) == 1
