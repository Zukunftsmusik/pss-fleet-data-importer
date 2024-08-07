import asyncio
import io
import logging
import logging.config
from contextlib import AbstractAsyncContextManager
from typing import AsyncGenerator, Optional

import alembic.command
import sqlalchemy_utils
from alembic.config import Config as AlembicConfig
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, async_scoped_session, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import text
from sqlmodel import SQLModel, create_engine
from sqlmodel.ext.asyncio.session import AsyncSession

from ..core import config
from .models import *  # noqa: F403, F401


class Database:
    def __init__(self, async_connection_string: str, sync_connection_string: str, echo: bool = False):
        self.async_connection_string = async_connection_string
        self.sync_connection_string = sync_connection_string
        self.echo = echo or False

        self.logger.info("Setting up async database engine for: %s", async_connection_string.split("@")[-1])
        self.async_engine: AsyncEngine = create_async_engine(
            async_connection_string,
            echo=self.echo,
            future=True,
            poolclass=NullPool,
        )
        async_session_factory = async_sessionmaker(bind=self.async_engine, class_=AsyncSession)
        self.async_scoped_session = async_scoped_session(async_session_factory, scopefunc=asyncio.current_task)

    @property
    def logger(self) -> logging.Logger:
        result = config.get_config().logger.getChild(Database.__qualname__)
        result.setLevel(config.get_config().log_level)
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
        connection: AsyncConnection = await self.async_engine.connect()
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
        reinitialize: Optional[bool] = False,
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

        if not sqlalchemy_utils.database_exists(sync_connection_string):
            self.logger.info("Creating database `%s`.", db_name)
            sqlalchemy_utils.create_database(sync_connection_string)
            self.logger.info("Database `%s` created.", db_name)

        if reinitialize:
            self.logger.info("Dropping tables of database `%s` as requested.", db_name)
            self.__drop_tables(sync_connection_string)
            self.logger.info("Tables of database `%s` dropped.", db_name)

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

    def __drop_tables(self, sync_connection_string: Optional[str] = None):
        sync_connection_string = sync_connection_string or self.sync_connection_string

        engine = create_engine(sync_connection_string, poolclass=NullPool)
        SQLModel.metadata.drop_all(engine)  # Drop all tables in the current schema

        with engine.begin() as connection:
            connection.execute(text("DROP TABLE IF EXISTS alembic_version;"))  # Drop alembic table, too

        engine.dispose()


__DATABASE = Database(
    config.get_config().db_async_connection_str,
    config.get_config().db_sync_connection_str,
    config.get_config().db_engine_echo,
)


class AsyncAutoRollbackSession(AbstractAsyncContextManager):
    # async_session_factory = None

    def __init__(self, database: Database):
        self.__database = database
        # self.__connection: AsyncConnection = None
        self.__session: AsyncSession = None
        self.__logger: logging.Logger = config.get_config().logger.getChild(AsyncAutoRollbackSession.__name__)
        # if not self.async_session_factory:
        #     self.async_session_factory = sessionmaker(self.__async_engine, class_=AsyncSession)
        # self.__async_scoped_session = async_scoped_session(DATABASE.async_session_factory, scopefunc=current_task)

    async def __aenter__(self):
        async with self.__database.async_scoped_session() as self.__session:
            # self.__connection = await self.__async_engine.connect()
            # async with self.__async_engine.connect() as self.__connection:
            #     async with AsyncSession(bind=self.__connection) as self.session:
            async with self.__session.begin():
                return self.__session

    async def __aexit__(self, exc_type, exception, _):
        if exception and exc_type is DBAPIError:
            self.__logger.error("An error occured during a database transaction. Rolling back session.", exc_info=exception, stack_info=True)
            await self.__session.rollback()
        await self.__session.close()
        # await self.__connection.close()


def get_db():
    return __DATABASE


__all__ = [
    AsyncAutoRollbackSession.__name__,
    Database.__name__,
    get_db.__name__,
]
