from datetime import datetime

import pytest

from src.importer.core.utils import extract_timestamp_from_gdrive_file_name


test_cases_valid = [
    # file_name: str, expected_result: datetime
    pytest.param("pss-top-100_20160106-235900.json", datetime(2016, 1, 6, 23, 59), id="valid_1"),
    pytest.param("pss-top-100_20241231-000004.json", datetime(2024, 12, 31, 0, 0, 4), id="valid_2"),
    pytest.param("pss-top-100-20160106-235900.json", datetime(2016, 1, 6, 23, 59), id="hyphen_instead_of_underscore_1"),
    pytest.param("pss-top-100-20241231-000004.json", datetime(2024, 12, 31, 0, 0, 4), id="hyphen_instead_of_underscore_2"),
]
"""file_name: str, expected_result: datetime"""


test_cases_invalid = [
    # file_name: str, expected_exception: Exception
    pytest.param(None, TypeError, id="none"),
    pytest.param("pss-top-100_2024121-000000.json", ValueError, id="too_short"),
    pytest.param("apss-top-100_20160106-235900.json", ValueError, id="too_long"),
    pytest.param("pss-top-100_20241301-000000.json", ValueError, id="month"),
    pytest.param("xxx-XXX-xxx_2024121-000000.json", ValueError, id="format"),
]
"""file_name: str, expected_exception: Exception"""


@pytest.mark.parametrize(["file_name", "expected_result"], test_cases_valid)
def test_extract_timestamp_from_gdrive_file_name(file_name: str, expected_result: datetime):
    result = extract_timestamp_from_gdrive_file_name(file_name)
    assert result == expected_result


@pytest.mark.parametrize(["file_name", "expected_exception"], test_cases_invalid)
def test_extract_timestamp_from_gdrive_file_name_invalid(file_name: str, expected_exception: Exception):
    with pytest.raises(expected_exception):
        _ = extract_timestamp_from_gdrive_file_name(file_name)
