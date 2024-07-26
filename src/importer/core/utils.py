from datetime import datetime, timedelta, timezone
from typing import Optional


def get_next_full_hour(now: Optional[datetime] = None) -> datetime:
    now = remove_timezone(now or get_now())
    result = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    return result


def get_now() -> datetime:
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


__all__ = [
    get_next_full_hour.__name__,
    get_now.__name__,
    remove_timezone.__name__,
]
