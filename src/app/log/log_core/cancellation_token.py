import logging
from typing import Any, Optional

from .. import LOGGER_BASE


LOGGER = LOGGER_BASE.getChild("cancelToken")


def if_cancelled(
    log_message: str,
    *log_message_args: Any,
    log_level: Optional[int] = logging.WARN,
):
    if log_level is None:
        log_level = logging.WARN
    LOGGER.log(log_level, log_message, *log_message_args)
