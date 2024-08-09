import logging

import pydrive2.files
import pytest

from src.app.core.gdrive import GoogleDriveClient
from src.app.importer.importer import download_gdrive_file_contents
from src.app.models.cancellation_token import CancellationToken, OperationCancelledError


@pytest.mark.usefixtures("patch_time_sleep")
@pytest.mark.usefixtures("patch_gdrive_client_get_file_contents")
def test_returns_contents(
    gdrive_file: pydrive2.files.GoogleDriveFile,
    mock_gdrive_client: GoogleDriveClient,
    cancel_token: CancellationToken,
    logger: logging.Logger,
    caplog: pytest.LogCaptureFixture,
    gdrive_file_contents: str,
):
    for max_download_attempts in range(1, 6):
        with caplog.at_level(logging.WARN):
            contents = download_gdrive_file_contents(gdrive_file, mock_gdrive_client, cancel_token, 1, max_download_attempts, False, logger)
        assert contents == gdrive_file_contents
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
    gdrive_file: pydrive2.files.GoogleDriveFile,
    mock_gdrive_client: GoogleDriveClient,
    cancel_token: CancellationToken,
    logger: logging.Logger,
    google_api_errors: dict[type[Exception], Exception],
    exception_type: type[Exception],
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
):
    def mock_gdrive_client_get_file_contents(*args):
        raise google_api_errors[exception_type]

    monkeypatch.setattr(GoogleDriveClient, GoogleDriveClient.get_file_contents.__name__, mock_gdrive_client_get_file_contents)

    item_no = 1337

    for max_download_attempts in range(1, 6):
        with caplog.at_level(logging.WARN):
            with pytest.raises(exception_type):
                _ = download_gdrive_file_contents(gdrive_file, mock_gdrive_client, cancel_token, item_no, max_download_attempts, False, logger)
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


@pytest.mark.usefixtures("patch_gdrive_client_new", "patch_time_sleep")
@pytest.mark.parametrize(["exception_type"], test_cases_raised_error_not_caught)
def test_raised_error_not_caught(
    gdrive_file: pydrive2.files.GoogleDriveFile,
    mock_gdrive_client: GoogleDriveClient,
    cancel_token: CancellationToken,
    logger: logging.Logger,
    exception_type: type[Exception],
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
):
    def mock_gdrive_client_get_file_contents(*args):
        raise exception_type()

    monkeypatch.setattr(GoogleDriveClient, GoogleDriveClient.get_file_contents.__name__, mock_gdrive_client_get_file_contents)

    item_no = 1337

    for max_download_attempts in range(1, 6):
        with caplog.at_level(logging.WARN):
            with pytest.raises(exception_type):
                _ = download_gdrive_file_contents(gdrive_file, mock_gdrive_client, cancel_token, item_no, max_download_attempts, False, logger)
        assert not caplog.text
        caplog.clear()


@pytest.mark.usefixtures("patch_gdrive_client_new", "patch_time_sleep")
def test_cancelled(
    gdrive_file: pydrive2.files.GoogleDriveFile,
    mock_gdrive_client: GoogleDriveClient,
    cancel_token: CancellationToken,
    logger: logging.Logger,
    caplog: pytest.LogCaptureFixture,
):
    cancel_token.cancel()
    item_no = 1337

    for max_download_attempts in range(1, 6):
        with caplog.at_level(logging.WARN):
            with pytest.raises(OperationCancelledError):
                _ = download_gdrive_file_contents(gdrive_file, mock_gdrive_client, cancel_token, item_no, max_download_attempts, False, logger)
        assert not caplog.text
        caplog.clear()
