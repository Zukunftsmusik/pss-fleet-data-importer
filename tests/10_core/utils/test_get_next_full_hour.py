from datetime import datetime, timezone

import pytest

from src.app.core.utils import get_next_full_hour


test_cases = [
    # dt: datetime, expected_result: datetime
    pytest.param(datetime(2016, 1, 6), datetime(2016, 1, 6, 1), id="full_hour"),
    pytest.param(datetime(2016, 1, 6, 0, 30), datetime(2016, 1, 6, 1), id="half_hour"),
    pytest.param(datetime(2016, 1, 6, 0, 0, 30), datetime(2016, 1, 6, 1), id="half_minute"),
    pytest.param(datetime(2016, 1, 6, tzinfo=timezone.utc), datetime(2016, 1, 6, 1), id="timezone_utc"),
    pytest.param(datetime(2016, 1, 6, 0, 59, 59), datetime(2016, 1, 6, 1), id="close_to_full_hour"),
]
"""dt: datetime, expected_result: datetime"""


@pytest.mark.parametrize(["dt", "expected_result"], test_cases)
def test_get_next_full_hour(dt: datetime, expected_result: datetime):
    result = get_next_full_hour(dt)
    assert result == expected_result
