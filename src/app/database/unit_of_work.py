import abc

from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_scoped_session
from sqlmodel.ext.asyncio.session import AsyncSession

from ..adapters.repository import AbstractCollectionFileRepository, SqlModelCollectionFileRepository
from ..log.log_database import async_auto_rollback_session as log
from .db_repository import DatabaseRepository


class AbstractUnitOfWork(abc.ABC):
    collection_files: AbstractCollectionFileRepository

    async def __aenter__(self) -> "AbstractUnitOfWork":
        return self

    async def __aexit__(self, exc_type, exception, _):
        if exception and exc_type is DBAPIError:
            log.transaction_error(exception)
            await self.rollback()

    @abc.abstractmethod
    async def commit(self):
        raise NotImplementedError

    @abc.abstractmethod
    async def rollback(self):
        raise NotImplementedError


class SqlModelUnitOfWork(AbstractUnitOfWork):
    def __init__(self, session_factory=None):
        self.session_factory: async_scoped_session[AsyncSession] = session_factory or DatabaseRepository.get_db().async_scoped_session

    async def __aenter__(self):
        async with self.session_factory() as self.session:
            async with self.session.begin():
                self.collection_files = SqlModelCollectionFileRepository(self.session)
                return await super().__aenter__()

    async def __aexit__(self, exc_type, exception, _):
        await super().__aexit__(exc_type, exception, _)
        await self.session.close()

    async def commit(self):
        await self.session.commit()

    async def rollback(self):
        await self.session.rollback()
