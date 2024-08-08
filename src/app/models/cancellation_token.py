import logging

from cancel_token import CancellationToken as CT


class OperationCanceledError(Exception):
    pass


class CancellationToken(CT):
    def raise_if_cancelled(
        self,
        logger: logging.Logger,
        log_message: str,
        *log_message_args,
        log_level: int = logging.WARN,
        exception_message: str = None,
    ):
        if self.cancelled:
            logger.log(log_level, log_message, *log_message_args)
            raise OperationCanceledError(exception_message or "")
