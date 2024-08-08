import logging
from typing import Any

from cancel_token import CancellationToken as CT


class OperationCanceledError(Exception):
    pass


class CancellationToken(CT):
    def log_if_cancelled(
        self,
        logger: logging.Logger,
        log_message: str,
        *log_message_args: Any,
        log_level: int = logging.WARN,
    ) -> bool:
        if self.cancelled:
            logger.log(log_level, log_message, *log_message_args)
        return self.cancelled

    def raise_if_cancelled(
        self,
        logger: logging.Logger = None,
        log_message: str = None,
        *log_message_args: Any,
        log_level: int = logging.WARN,
        exception_message: str = None,
    ) -> bool:
        if self.cancelled:
            if logger and log_message:
                logger.log(log_level, log_message, *log_message_args)
            raise OperationCanceledError(exception_message or "")
        return False


__all__ = [
    CancellationToken.__name__,
    OperationCanceledError.__name__,
]
