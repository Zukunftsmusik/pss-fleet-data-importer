import abc
from datetime import datetime
from typing import Iterable, Optional

from sqlalchemy.sql.operators import is_
from sqlmodel import asc, col, select
from sqlmodel.ext.asyncio.session import AsyncSession

from ..database import crud
from ..database.models import CollectionFileDB


class AbstractCollectionFileRepository(abc.ABC):
    @abc.abstractmethod
    def add(self, collection_file: CollectionFileDB):
        raise NotImplementedError

    @abc.abstractmethod
    async def get_by_id(self, collection_file_id: int) -> Optional[CollectionFileDB]:
        raise NotImplementedError

    @abc.abstractmethod
    async def get_latest_imported_gdrive_modified_date(self) -> Optional[datetime]:
        raise NotImplementedError

    @abc.abstractmethod
    async def list_files(self, imported: Optional[bool] = None, gdrive_file_ids: Optional[list[str]] = None) -> list[CollectionFileDB]:
        raise NotImplementedError

    @abc.abstractmethod
    async def refresh_files(self, collection_files: Iterable[CollectionFileDB]) -> list[CollectionFileDB]:
        raise NotImplementedError


class SqlModelCollectionFileRepository(AbstractCollectionFileRepository):
    def __init__(self, session: AsyncSession):
        self.session = session

    def add(self, collection_file: CollectionFileDB):
        self.session.add(collection_file)

    async def get_by_id(self, collection_file_id: int) -> Optional[CollectionFileDB]:
        return await crud.get_collection_file_by_id(self.session, collection_file_id)

    async def get_latest_imported_gdrive_modified_date(self) -> Optional[datetime]:
        return await crud.get_latest_imported_gdrive_modified_date(self.session)

    async def list_files(self, imported: Optional[bool] = None, gdrive_file_ids: Optional[list[str]] = None) -> list[CollectionFileDB]:
        async with self.session:
            query = select(CollectionFileDB).order_by(asc(CollectionFileDB.timestamp))

            if imported is not None:
                query = query.where(is_(CollectionFileDB.imported, imported))

            if gdrive_file_ids:
                query = query.where(col(CollectionFileDB.gdrive_file_id).in_(gdrive_file_ids))

            return (await self.session.exec(query)).all()

    async def refresh_files(self, collection_files: Iterable[CollectionFileDB]) -> list[CollectionFileDB]:
        for collection_file in collection_files:
            await self.session.refresh(collection_file)

        return collection_files
