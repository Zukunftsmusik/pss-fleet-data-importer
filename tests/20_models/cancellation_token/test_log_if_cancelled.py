import logging

import pytest

from src.app.models.cancellation_token import CancellationToken


test_cases_not_cancelled = [
    # log_level: int, capture_log_level: int
    pytest.param(logging.DEBUG, logging.WARN, id="debug_warn"),
    pytest.param(logging.INFO, logging.WARN, id="info_warn"),
    pytest.param(logging.WARN, logging.WARN, id="warn_warn"),
    pytest.param(logging.ERROR, logging.WARN, id="error_warn"),
]
"""log_level: int, capture_log_level: int"""


test_cases_cancelled_captured = [
    # log_message: str, log_level: int, capture_log_level: int
    pytest.param("Cancelled", logging.INFO, logging.DEBUG, id="info_debug"),
    pytest.param("Cancelled", logging.INFO, logging.INFO, id="info_info"),
]
"""log_message: str, log_level: int, capture_log_level: int"""


test_cases_cancelled_not_captured = [
    # log_level: int, capture_log_level: int
    pytest.param(logging.DEBUG, logging.WARN, id="debug_warn"),
    pytest.param(logging.INFO, logging.WARN, id="info_warn"),
]
"""log_level: int, capture_log_level: int"""


@pytest.mark.parametrize(["log_message", "log_level", "capture_log_level"], test_cases_cancelled_captured)
def test_capture_logs(
    log_message: str,
    log_level: int,
    capture_log_level: int,
    logger: logging.Logger,
    caplog: pytest.LogCaptureFixture,
):
    token = CancellationToken()
    token.cancel()

    with caplog.at_level(capture_log_level):
        cancelled = token.log_if_cancelled(logger, log_message, log_level=log_level)
    assert cancelled is True
    assert log_message in caplog.text


@pytest.mark.parametrize(["log_level", "capture_log_level"], test_cases_cancelled_not_captured)
def test_capture_no_logs(
    log_level: int,
    capture_log_level: int,
    logger: logging.Logger,
    caplog: pytest.LogCaptureFixture,
):
    token = CancellationToken()
    token.cancel()

    with caplog.at_level(capture_log_level):
        cancelled = token.log_if_cancelled(logger, "log_message", log_level=log_level)
    assert cancelled is True
    assert not caplog.text


@pytest.mark.parametrize(["log_level", "capture_log_level"], test_cases_not_cancelled)
def test_not_cancelled(
    log_level: int,
    capture_log_level: int,
    logger: logging.Logger,
    caplog: pytest.LogCaptureFixture,
):
    token = CancellationToken()

    with caplog.at_level(capture_log_level):
        cancelled = token.log_if_cancelled(logger, "log_message", log_level=log_level)
    assert cancelled is False
    assert not caplog.text
