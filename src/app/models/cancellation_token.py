import logging
from typing import Any

from cancel_token import CancellationToken as CT


class OperationCanceledError(Exception):
    pass


class CancellationToken(CT):
    def raise_if_cancelled(
        self,
        logger: logging.Logger = None,
        log_message: str = None,
        *log_message_args: Any,
        log_level: int = logging.WARN,
        exception_message: str = None,
    ):
        if self.cancelled:
            if logger and log_message:
                logger.log(log_level, log_message, *log_message_args)
            raise OperationCanceledError(exception_message or "")
