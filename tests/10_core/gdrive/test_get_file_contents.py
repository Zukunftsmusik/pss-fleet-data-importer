import logging
from typing import Optional

import googleapiclient.errors
import pydrive2.files
import pytest

from src.app.core.gdrive import get_file_contents


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


test_cases_valid = [
    # content: Optional[str]
    pytest.param(None, id="none"),
    pytest.param("", id="str_empty"),
    pytest.param("content", id="str_content"),
    pytest.param("{}", id="json_empty"),
    pytest.param("null", id="json_null"),
]
"""content: Optional[str]"""


@pytest.mark.parametrize(["exception_type"], test_cases_do_not_log_exception)
def test_do_not_log_exception(
    exception_type: type[Exception],
    logger: logging.Logger,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    def mock_GetContentString(*args):
        raise exception_type()

    monkeypatch.setattr(pydrive2.files.GoogleDriveFile, pydrive2.files.GoogleDriveFile.GetContentString.__name__, mock_GetContentString)

    file: pydrive2.files.GoogleDriveFile = pydrive2.files.GoogleDriveFile(None)

    with caplog.at_level(logging.WARNING):
        with pytest.raises(exception_type):
            _ = get_file_contents(file, logger)

    assert "An error occured while downloading file" not in caplog.text


@pytest.mark.parametrize(["exception_type", "exception_instance"], test_cases_log_exception)
def test_log_exception(
    exception_type: type[Exception],
    exception_instance: Exception,
    logger: logging.Logger,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    def mock_GetContentString(*args):
        raise exception_instance

    monkeypatch.setattr(pydrive2.files.GoogleDriveFile, pydrive2.files.GoogleDriveFile.GetContentString.__name__, mock_GetContentString)

    file_name = "file_name"
    file: pydrive2.files.GoogleDriveFile = pydrive2.files.GoogleDriveFile(None, {"title": file_name})

    with caplog.at_level(logging.WARNING):
        with pytest.raises(exception_type):
            _ = get_file_contents(file, logger)

    assert file_name in caplog.text
    assert "An error occured while downloading file" in caplog.text


@pytest.mark.parametrize(["content"], test_cases_valid)
def test_returns_contents(
    content: Optional[str],
    logger: logging.Logger,
    monkeypatch: pytest.MonkeyPatch,
):
    def mock_GetContentString(*args):
        return content

    monkeypatch.setattr(pydrive2.files.GoogleDriveFile, pydrive2.files.GoogleDriveFile.GetContentString.__name__, mock_GetContentString)

    file: pydrive2.files.GoogleDriveFile = pydrive2.files.GoogleDriveFile(None)
    result = get_file_contents(file, logger)
    assert result == content
