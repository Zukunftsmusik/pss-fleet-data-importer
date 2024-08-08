import logging
from time import perf_counter
from typing import Any, Callable


def debug_log_running_time(logger: logging.Logger, message: str) -> Callable[..., Any]:
    def wrapper(func: Callable[..., Any], *args, **kwargs) -> Any:
        start = perf_counter()
        result = func(*args, **kwargs)
        end = perf_counter()
        logger.debug("%s took %.2f seconds", message, end - start)
        return result

    return wrapper
