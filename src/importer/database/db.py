import io
import logging
import logging.config
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import AsyncGenerator, Optional

import alembic.command
import sqlalchemy_utils
from alembic.config import Config as AlembicConfig
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
from sqlmodel.ext.asyncio.session import AsyncSession

from ..core.config import CONFIG


class AsyncAutoRollbackSession(AbstractAsyncContextManager):
    def __init__(self, async_engine: AsyncEngine):
        self.__async_engine = async_engine
        self.__connection: AsyncConnection = None
        self.session: AsyncSession = None
        self.__logger: logging.Logger = CONFIG.logger.getChild(AsyncAutoRollbackSession.__name__)

    async def __aenter__(self):
        self.__connection = await self.__async_engine.connect()
        async with AsyncSession(bind=self.__connection) as async_session:
            self.session = async_session
            return self.session

    async def __aexit__(self, exc_type, exception, traceback):
        if exception and isinstance(exception, DBAPIError):
            self.__logger.error("An error occured during a database transaction. Rolling back session.", exc_info=exception, stack_info=True)
            await self.session.rollback()
        await self.session.close()
        await self.__connection.close()


class Database:
    async_connection_string: str = None
    echo: bool = False
    sync_connection_string: str = None

    def __init__(self, async_connection_string: str, sync_connection_string: str, echo: bool = False):
        self.async_connection_string = async_connection_string
        self.sync_connection_string = sync_connection_string
        self.echo = echo or False

        self.logger.info("Setting up async database engine for: %s", async_connection_string.split("@")[-1])
        self.__async_engine: AsyncEngine = create_async_engine(async_connection_string, echo=self.echo, future=True)

    @property
    def async_engine(self) -> AsyncEngine:
        return self.__async_engine

    @property
    def logger(self) -> logging.Logger:
        result = CONFIG.logger.getChild(Database.__qualname__)
        result.setLevel(CONFIG.log_level)
        return result

    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Creates and returns an `AsyncSession` from the `self.async_engine` in this class. If an error occurs during a session, the changes will be rolled back and the exception will be raised again.

        Raises:
            DBAPIError: Raised, if an error occurs during a transaction.

        Returns:
            AsyncGenerator[AsyncSession, None]: _description_

        Yields:
            AsyncSession: The created `AsyncSession` object.
        """
        connection: AsyncConnection = await self.__async_engine.connect()
        try:
            async with AsyncSession(bind=connection) as async_session:
                try:
                    yield async_session
                except DBAPIError as session_exception:
                    await async_session.rollback()
                    raise session_exception
                except KeyboardInterrupt as exc:
                    await async_session.rollback()
                    raise exc
                finally:
                    await async_session.close()
        except DBAPIError as connection_exception:
            self.logger.error("An error occured during a database transaction.", exc_info=connection_exception, stack_info=True)
            raise connection_exception
        except KeyboardInterrupt as exc:
            raise exc
        finally:
            await connection.close()

    def initialize_database(
        self,
        sync_connection_string: Optional[str] = None,
        async_connection_string: Optional[str] = None,
        echo: Optional[bool] = None,
        drop_tables: Optional[bool] = False,
    ):
        """Initializes the database. Optionally drops all tables before creating them. Optionally dummy data will be read from disk and inserted.

        Args:
            sync_connection_string (str, optional): The connections string for `synchronous` connection to the database. Defaults to `self.sync_connection_string`.
            async_connection_string (str, optional): The connections string for an `asynchronous` connection to the database. Defaults to `self.async_connection_string`.
            echo (bool, optional): Determines, if SQL statements should be logged to `stdout`. Defaults to `self.echo`.
            drop_tables (bool, optional): Determines, if all tables should be dropped before being recreated. Defaults to False.
        """
        sync_connection_string = sync_connection_string or self.sync_connection_string
        async_connection_string = async_connection_string or self.async_connection_string
        echo = echo or self.echo
        db_name = sync_connection_string.split("/")[-1]

        if drop_tables and sqlalchemy_utils.database_exists(sync_connection_string):
            self.logger.info("Dropping database `%s` as requested.", db_name)
            sqlalchemy_utils.drop_database(sync_connection_string)
            self.logger.info("Database `%s` dropped.", db_name)

        if not sqlalchemy_utils.database_exists(sync_connection_string):
            self.logger.info("Creating database `%s`.", db_name)
            sqlalchemy_utils.create_database(sync_connection_string)
            self.logger.info("Database `%s` created.", db_name)

        if not self.alembic_current_is_head(sync_connection_string=sync_connection_string):
            self.logger.info("Applying migrations to database `%s`.", db_name)
            alembic_config = AlembicConfig("alembic.ini")
            alembic_config.attributes["sqlalchemy.url"] = sync_connection_string
            alembic.command.upgrade(alembic_config, "head", tag="from_app")

        self.logger.info("Database `%s` schema is up-to-date.", db_name)

    def alembic_current_is_head(self, sync_connection_string: Optional[str] = None):
        sync_connection_string = sync_connection_string or self.sync_connection_string
        output_buffer = io.StringIO()

        alembic_config = AlembicConfig("alembic.ini", stdout=output_buffer)
        alembic_config.attributes["sqlalchemy.url"] = sync_connection_string

        alembic.command.current(alembic_config)
        current = output_buffer.getvalue()

        return "(head)" in current


DATABASE = Database(CONFIG.db_async_connection_str, CONFIG.db_sync_connection_str, CONFIG.db_engine_echo)


__all__ = [
    AsyncAutoRollbackSession.__name__,
    "DATABASE",
]
