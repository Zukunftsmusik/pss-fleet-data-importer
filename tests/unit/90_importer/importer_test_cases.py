import pytest
from pydrive2.files import ApiRequestError, FileNotDownloadableError


test_cases_raised_error_caught = [
    # exception_type
    pytest.param(ApiRequestError, id="api_request_error"),
    pytest.param(FileNotDownloadableError, id="file_not_downloadable_error"),
]
"""exception_type: type[Exception]"""
