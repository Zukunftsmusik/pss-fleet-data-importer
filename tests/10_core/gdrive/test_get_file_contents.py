import logging

import googleapiclient.errors
import pydrive2.files
import pytest

from mock_classes import MockGDriveFile, MockGoogleDriveClient


class MockResponse:
    reason: str = None
    status: int = 400


test_cases_do_not_log_exception = [
    # exception_type: type[Exception]
    pytest.param(Exception, id="exception"),
    pytest.param(IOError, id="io_error"),
    pytest.param(ValueError, id="value_error"),
    pytest.param(TypeError, id="type_error"),
]
"""exception_type: type[Exception]"""


test_cases_log_exception = [
    # exception_type: type[Exception], exception_instance: Exception
    pytest.param(
        pydrive2.files.ApiRequestError,
        pydrive2.files.ApiRequestError(googleapiclient.errors.HttpError(MockResponse(), b"{}")),
        id="api_request_error",
    ),
    pytest.param(pydrive2.files.FileNotDownloadableError, pydrive2.files.FileNotDownloadableError(), id="file_not_downloadable_error"),
]
"""exception_type: type[Exception], exception_instance: Exception"""


@pytest.mark.parametrize(["exception_type"], test_cases_do_not_log_exception)
def test_do_not_log_exception(
    mock_gdrive_client: MockGoogleDriveClient,
    mock_gdrive_file: MockGDriveFile,
    exception_type: type[Exception],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    def mock_get_content_string(*args):
        raise exception_type()

    monkeypatch.setattr(MockGDriveFile, MockGDriveFile.get_content_string.__name__, mock_get_content_string)

    with caplog.at_level(logging.WARNING):
        with pytest.raises(exception_type):
            _ = mock_gdrive_client.get_file_contents(mock_gdrive_file)

    assert "An error occured while downloading file" not in caplog.text


@pytest.mark.parametrize(["exception_type", "exception_instance"], test_cases_log_exception)
def test_log_exception(
    mock_gdrive_client: MockGoogleDriveClient,
    mock_gdrive_file: MockGDriveFile,
    exception_type: type[Exception],
    exception_instance: Exception,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    def mock_get_content_string(*args):
        raise exception_instance

    monkeypatch.setattr(MockGDriveFile, MockGDriveFile.get_content_string.__name__, mock_get_content_string)

    with caplog.at_level(logging.WARNING):
        with pytest.raises(exception_type):
            _ = mock_gdrive_client.get_file_contents(mock_gdrive_file)

    assert mock_gdrive_file.name in caplog.text
    assert "An error occured while downloading file" in caplog.text


def test_returns_contents(
    mock_gdrive_client: MockGoogleDriveClient,
    mock_gdrive_file: MockGDriveFile,
):
    result = mock_gdrive_client.get_file_contents(mock_gdrive_file)
    assert result == mock_gdrive_file.content
