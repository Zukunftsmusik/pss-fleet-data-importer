import logging

import pydrive2.files
import pytest

from mock_classes import MockGDriveFile, MockGoogleDriveClient
from src.app.core.gdrive import GoogleDriveClient
from src.app.core.models.exceptions import OperationCancelledError
from src.app.importer.download_worker import download_gdrive_file_contents
from src.app.models import CancellationToken


@pytest.mark.usefixtures("patch_time_sleep")
def test_returns_contents(
    random_gdrive_file: MockGDriveFile,
    mock_gdrive_client: GoogleDriveClient,
    cancel_token: CancellationToken,
    caplog: pytest.LogCaptureFixture,
):
    for max_download_attempts in range(1, 6):
        with caplog.at_level(logging.WARN):
            contents = download_gdrive_file_contents(random_gdrive_file, mock_gdrive_client, cancel_token, 1, max_download_attempts, False)
        assert contents == random_gdrive_file.content
        assert not caplog.text
        caplog.clear()


test_cases_raised_error_caught = [
    # exception_type
    pytest.param(pydrive2.files.ApiRequestError, id="api_request_error"),
    pytest.param(pydrive2.files.FileNotDownloadableError, id="file_not_downloadable_error"),
]
"""exception_type"""


@pytest.mark.usefixtures("patch_time_sleep")
@pytest.mark.parametrize(["exception_type"], test_cases_raised_error_caught)
def test_raised_error_caught(
    random_gdrive_file: MockGDriveFile,
    mock_gdrive_client: MockGoogleDriveClient,
    cancel_token: CancellationToken,
    google_api_errors: dict[type[Exception], Exception],
    exception_type: type[Exception],
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
):
    def mock_gdrive_client_get_file_contents(*args):
        raise google_api_errors[exception_type]

    monkeypatch.setattr(MockGoogleDriveClient, MockGoogleDriveClient.get_file_contents.__name__, mock_gdrive_client_get_file_contents)

    item_no = 1337

    for max_download_attempts in range(1, 6):
        with caplog.at_level(logging.WARN):
            with pytest.raises(exception_type):
                _ = download_gdrive_file_contents(random_gdrive_file, mock_gdrive_client, cancel_token, item_no, max_download_attempts, False)
        assert caplog.text
        assert str(item_no) in caplog.text
        caplog.clear()


test_cases_raised_error_not_caught = [
    # exception_type
    pytest.param(Exception, id="exception"),
    pytest.param(IOError, id="io_error"),
    pytest.param(TypeError, id="type_error"),
    pytest.param(ValueError, id="value_error"),
    pytest.param(OperationCancelledError, id="operation_cancelled_error"),
]
"""exception_type"""


@pytest.mark.usefixtures("patch_time_sleep")
@pytest.mark.parametrize(["exception_type"], test_cases_raised_error_not_caught)
def test_raised_error_not_caught(
    random_gdrive_file: MockGDriveFile,
    mock_gdrive_client: GoogleDriveClient,
    cancel_token: CancellationToken,
    exception_type: type[Exception],
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
):
    def mock_gdrive_client_get_file_contents(*args):
        raise exception_type()

    monkeypatch.setattr(MockGoogleDriveClient, MockGoogleDriveClient.get_file_contents.__name__, mock_gdrive_client_get_file_contents)

    item_no = 1337

    for max_download_attempts in range(1, 6):
        with caplog.at_level(logging.WARN):
            with pytest.raises(exception_type):
                _ = download_gdrive_file_contents(random_gdrive_file, mock_gdrive_client, cancel_token, item_no, max_download_attempts, False)
        assert not caplog.text
        caplog.clear()


@pytest.mark.usefixtures("patch_time_sleep")
def test_cancelled(
    random_gdrive_file: MockGDriveFile,
    mock_gdrive_client: MockGoogleDriveClient,
    cancel_token: CancellationToken,
    caplog: pytest.LogCaptureFixture,
):
    cancel_token.cancel()
    item_no = 1337

    for max_download_attempts in range(1, 6):
        with caplog.at_level(logging.WARN):
            with pytest.raises(OperationCancelledError):
                _ = download_gdrive_file_contents(random_gdrive_file, mock_gdrive_client, cancel_token, item_no, max_download_attempts, False)
        assert not caplog.text
        caplog.clear()
