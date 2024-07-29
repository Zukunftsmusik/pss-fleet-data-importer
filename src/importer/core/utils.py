import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Coroutine, Iterable, Optional, Union

from pydrive2.files import GoogleDriveFile


def extract_timestamp_from_gdrive_file_name(file_name: str) -> datetime:
    """Extracts the timestamp from a PSS fleet data file name.

    Args:
        file_name (str): The name of the Collection file.

    Raises:
        ValueError: Raised if the length of `file_name` doesn't match the expected length.

    Returns:
        datetime: The extracted timestamp as a timezone-naive `datetime`.
    """
    format_string = "pss-top-100_%Y%m%d-%H%M%S.json"
    expected_file_name_length = len(format_string) + 2

    if len(file_name) != expected_file_name_length:
        raise ValueError(f"The provided file name is not of expected length: {expected_file_name_length}")

    timestamp = datetime.strptime(file_name, format_string)
    return timestamp


def get_gdrive_file_name(gdrive_file: GoogleDriveFile) -> str:
    """Returns the file name of a `GoogleDriveFile` of API version 2 or 3.

    Args:
        gdrive_file (GoogleDriveFile): The file to retrieve the file name from.

    Returns:
        str: The file name.
    """
    file_name = gdrive_file.get("title") or gdrive_file.get("name")  # "name" is gdrive API V3, "title" is V2
    return file_name


def get_next_full_hour(dt: datetime) -> datetime:
    """Get a `datetime` representing the next full hour relative to the input.

    Args:
        dt (datetime): The input.

    Returns:
        datetime: Represents the next full hour relative to the input.
    """
    dt = remove_timezone(dt)
    result = dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    return result


def get_now() -> datetime:
    """Get the current date & time in UTC, but without timezone information.

    Returns:
        datetime: The current time in UTC, but without timezone information.
    """
    return remove_timezone(datetime.now(tz=timezone.utc))


def remove_timezone(dt: Optional[datetime]) -> datetime:
    """Removes timezone information from a timezone-aware `datetime` object.

    Args:
        dt (datetime): The `datetime` to remove the timezone information from.

    Returns:
        datetime: A timezone-naive `datetime` object.
    """
    if dt is None:
        return None

    if not isinstance(dt, datetime):
        raise TypeError("The parameter `dt` must be of type `datetime`!")

    return dt.replace(tzinfo=None)


def run_async_thread_pool_executor(
    fn: Callable[..., Awaitable[None]],
    arguments: Union[Iterable[Iterable[Any]], Iterable[Any]],
    max_workers: int,
) -> ThreadPoolExecutor:
    """Run an async function with multiple threads in parallel, passing the given arguments.

    Args:
        fn (Callable[..., Awaitable[Any, Any, None]]): An async function taking an arbitrary number of positional arguments. This function returns nothing.
        arguments (Union[Iterable[Iterable[Any]], Iterable[Any]]): An iterable of arguments or list of arguments to be passed to `fn`.
        max_workers (int): The maximum number of worker threads to be used.

    Returns:
        ThreadPoolExecutor: An executor managing the thread pool to be waited on, closed, canceled etc.
    """
    executor = ThreadPoolExecutor(max_workers=max_workers)

    if arguments:
        if isinstance(arguments[0], Iterable):
            workers = [fn(*args) for args in arguments]
        else:
            workers = [fn(arg) for arg in arguments]
        executor.map(asyncio.run, workers)

    return executor


__all__ = [
    extract_timestamp_from_gdrive_file_name.__name__,
    get_gdrive_file_name.__name__,
    get_next_full_hour.__name__,
    get_now.__name__,
    remove_timezone.__name__,
    run_async_thread_pool_executor.__name__,
]
