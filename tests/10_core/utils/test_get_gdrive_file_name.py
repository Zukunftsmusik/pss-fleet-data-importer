import pytest
from pydrive2.files import GoogleDriveFile

from src.app.core.utils import get_gdrive_file_name


test_cases = [
    # gdrive_file: GoogleDriveFile, expected_result: str
    pytest.param(GoogleDriveFile(None, {}), None, id="empty"),
    pytest.param(GoogleDriveFile(None, {"title": None}), None, id="v2_None"),
    pytest.param(GoogleDriveFile(None, {"title": "Title"}), "Title", id="v2_title"),
    pytest.param(GoogleDriveFile(None, {"name": None}), None, id="v3_none"),
    pytest.param(GoogleDriveFile(None, {"name": "Name"}), "Name", id="v3_name"),
    pytest.param(GoogleDriveFile(None, {"title": "Title", "name": None}), "Title", id="mixed_title_and_name_is_none"),
    pytest.param(GoogleDriveFile(None, {"title": None, "name": "Name"}), "Name", id="mixed_title_is_none_and_name"),
    pytest.param(GoogleDriveFile(None, {"title": None, "name": None}), None, id="mixed_title_is_none_and_name_is_none"),
]
"""gdrive_file: GoogleDriveFile, expected_result: str"""


@pytest.mark.parametrize(["gdrive_file", "expected_result"], test_cases)
def test_get_gdrive_file_name(gdrive_file: GoogleDriveFile, expected_result: str):
    result = get_gdrive_file_name(gdrive_file)
    assert result == expected_result
