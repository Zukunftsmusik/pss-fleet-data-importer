from .. import LOGGER_BASE


LOGGER = LOGGER_BASE.getChild("AsyncAutoRollbackSession")


def transaction_error(exception: Exception):
    LOGGER.error("An error occured during a database transaction. Rolling back session.", exc_info=exception, stack_info=True)
