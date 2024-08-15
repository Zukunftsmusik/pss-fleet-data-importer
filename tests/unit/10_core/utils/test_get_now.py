import asyncio

from src.app.core.utils import get_now


async def test_get_now():
    now = get_now()
    assert now.tzinfo is None

    await asyncio.sleep(0.1)

    later = get_now()
    assert now != later
