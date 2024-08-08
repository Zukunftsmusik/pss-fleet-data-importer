import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable, Mapping, Optional, Union

from cancel_token import CancellationToken
from pydrive2.files import GoogleDriveFile

from ..models.status import StatusFlag


def extract_timestamp_from_gdrive_file_name(file_name: str) -> datetime:
    """Extracts the timestamp from a PSS fleet data file name.

    Args:
        file_name (str): The name of the Collection file.

    Raises:
        ValueError: Raised if the length of `file_name` doesn't match the expected length.

    Returns:
        datetime: The extracted timestamp as a timezone-naive `datetime`.
    """
    format_strings = [
        "pss-top-100_%Y%m%d-%H%M%S.json",
        "pss-top-100-%Y%m%d-%H%M%S.json",
    ]
    expected_file_name_lengths = [len(format_string) + 2 for format_string in format_strings]

    if len(file_name) not in expected_file_name_lengths:
        raise ValueError(f"The provided file name is not of any expected length: {expected_file_name_lengths}")

    for format_string in format_strings:
        try:
            timestamp = datetime.strptime(file_name, format_string)
            return timestamp
        except ValueError:
            pass
    raise ValueError(f"The provided file name did not match any of the expected formats: {format_strings}")


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


def is_empty_file(file_path: Union[Path, str]) -> bool:
    file_path = Path(file_path)
    return file_path.stat().st_size < 1


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


def start_pooled_bulk_operation(
    thread_pool_size: int,
    worker_func: Callable,
    worker_items: Iterable[Any],
    cancel_token: CancellationToken = None,
    status_flag: StatusFlag = None,
):
    if status_flag is not None:
        status_flag.value = True

    with ThreadPoolExecutor(thread_pool_size) as executor:
        for item in worker_items:
            if cancel_token and cancel_token.cancelled:
                return

            executor.submit(worker_func, item)

    if status_flag is not None:
        status_flag.value = False


def create_async_thread(
    coro: Callable[..., Awaitable[Any]],
    name: str = None,
    args: Iterable[Any] = None,
    kwargs: Mapping[str, Any] = None,
    daemon: bool = True,
) -> threading.Thread:
    args = args or ()
    kwargs = kwargs or {}

    def target():
        asyncio.run(coro(*args, **kwargs))

    thread = threading.Thread(target=target, name=name, daemon=daemon)
    return thread


__all__ = [
    create_async_thread.__name__,
    extract_timestamp_from_gdrive_file_name.__name__,
    get_gdrive_file_name.__name__,
    get_next_full_hour.__name__,
    get_now.__name__,
    remove_timezone.__name__,
    start_pooled_bulk_operation.__name__,
]
