from datetime import datetime

import pytest
from dateutil.parser import parse as parse_datetime

from src.importer.core.utils import remove_timezone


test_cases_invalid = [
    # value, expected_exception
    pytest.param(True, TypeError, id="bool"),
    pytest.param(12.34, TypeError, id="float"),
    pytest.param(1234, TypeError, id="int"),
    pytest.param("1234", TypeError, id="str"),
    pytest.param(complex("-1.23+4.5j"), TypeError, id="complex"),
    pytest.param([5020], TypeError, id="list[int]"),
    pytest.param({"seconds": 5020}, TypeError, id="dict[str, int]"),
    pytest.param((datetime(2016, 1, 1),), TypeError, id="tuple[datetime]"),
]


test_cases_valid = [
    # value, expected_result
    pytest.param(None, None, id="none"),
    pytest.param(parse_datetime("2016-01-07T01:23:40"), parse_datetime("2016-01-07T01:23:40"), id="no_timezone"),
    pytest.param(parse_datetime("2016-01-07T01:23:40Z"), parse_datetime("2016-01-07T01:23:40"), id="timezone_utc"),
    pytest.param(parse_datetime("2016-01-07T01:23:40+02:00"), parse_datetime("2016-01-07T01:23:40"), id="timezone_mest"),
]


@pytest.mark.parametrize(["value", "expected_exception"], test_cases_invalid)
def test_remove_timezone_invalid(value, expected_exception):
    with pytest.raises(expected_exception):
        _ = remove_timezone(value)


@pytest.mark.parametrize(["value", "expected_result"], test_cases_valid)
def test_remove_timezone_valid(value, expected_result):
    result = remove_timezone(value)
    assert result == expected_result
