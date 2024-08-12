import logging
from typing import Any, Optional

from cancel_token import CancellationToken as CT

from ...log.log_core import cancellation_token as log


class OperationCancelledError(Exception):
    pass


class CancellationToken(CT):
    def log_if_cancelled(
        self,
        log_message: str,
        *log_message_args: Any,
        log_level: Optional[int] = logging.WARN,
    ) -> bool:
        if self.cancelled:
            log.if_cancelled(log_message, *log_message_args, log_level=log_level)
        return self.cancelled

    def raise_if_cancelled(
        self,
        log_message: Optional[str] = None,
        *log_message_args: Any,
        log_level: Optional[int] = logging.WARN,
        exception_message: Optional[str] = None,
    ) -> bool:
        if self.cancelled:
            if log_message:
                log.if_cancelled(log_message, *log_message_args, log_level=log_level)
            raise OperationCancelledError(exception_message or "")
        return False


__all__ = [
    # Classes
    CancellationToken.__name__,
    OperationCancelledError.__name__,
]
