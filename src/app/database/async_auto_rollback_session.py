from contextlib import AbstractAsyncContextManager

from sqlalchemy.exc import DBAPIError
from sqlmodel.ext.asyncio.session import AsyncSession

from ..log.log_database import async_auto_rollback_session as log
from .db import Database


class AsyncAutoRollbackSession(AbstractAsyncContextManager):
    def __init__(self, database: Database):
        self.__database: Database = database
        self.__session: AsyncSession = None

    async def __aenter__(self):
        async with self.__database.async_scoped_session() as self.__session:
            async with self.__session.begin():
                return self.__session

    async def __aexit__(self, exc_type, exception, _):
        if exception and exc_type is DBAPIError:
            log.transaction_error(exception)
            await self.__session.rollback()
        await self.__session.close()


__all__ = [
    # Classes
    AsyncAutoRollbackSession.__name__,
]
