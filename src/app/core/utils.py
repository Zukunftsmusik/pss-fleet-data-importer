import asyncio
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable, Mapping, Optional, Union

from .models.filesystem import FileSystem


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


def is_empty_file(file_path: Union[Path, str], filesystem: FileSystem = FileSystem()) -> bool:
    return filesystem.get_size(file_path) == 0


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


__all__ = [
    create_async_thread.__name__,
    extract_timestamp_from_gdrive_file_name.__name__,
    get_next_full_hour.__name__,
    get_now.__name__,
    is_empty_file.__name__,
    remove_timezone.__name__,
]
