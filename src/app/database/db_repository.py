from typing import AsyncGenerator

from sqlmodel.ext.asyncio.session import AsyncSession

from ..core import config
from .async_auto_rollback_session import AsyncAutoRollbackSession
from .db import Database
from .models import *  # noqa: F403, F401


class DatabaseRepository:
    __database: Database = None

    @classmethod
    def get_db(cls) -> Database:
        if not cls.__database:
            configuration = config.ConfigRepository.get_config()
            cls.__database = Database(
                configuration.db_sync_connection_str,
                configuration.db_async_connection_str,
                configuration.db_engine_echo,
            )
            cls.__database.initialize_database(reinitialize=configuration.reinitialize_database_on_startup)
        return cls.__database

    @classmethod
    def get_session(cls) -> AsyncGenerator[AsyncSession, None]:
        return AsyncAutoRollbackSession(cls.get_db())


__all__ = [
    DatabaseRepository.__name__,
]
