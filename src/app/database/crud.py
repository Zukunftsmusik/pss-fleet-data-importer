from datetime import datetime
from typing import Iterable, Optional

from sqlalchemy.sql.operators import is_, is_not
from sqlmodel import asc, col, desc, select
from sqlmodel.ext.asyncio.session import AsyncSession

from .models import CollectionFileDB


async def get_collection_file_by_id(session: AsyncSession, collection_file_id: int) -> Optional[CollectionFileDB]:
    """Retrieves the CollectionFile with the specified `collection_file_id`.

    Args:
        session (AsyncSession): The database session to use.
        collection_file_id (int): The `collection_file_id` of the CollectionFile to retrieve.

    Returns:
        Optional[CollectionFileDB]: The requested CollectionFile, if it exists. Else, None.
    """
    async with session:
        collection_file = await session.get(CollectionFileDB, collection_file_id)
        return collection_file


async def list_collection_files_by_gdrive_file_ids(session: AsyncSession, gdrive_file_ids: Iterable[str]) -> list[CollectionFileDB]:
    """Retrieves the CollectionFiles with any of the given `gdrive_file_id`s.

    Args:
        session (AsyncSession): The database session to use.
        gdrive_file_ids (Iterable[str]): A collection of `gdrive_file_id`s of the CollectionFiles to return.

    Returns:
        list[CollectionFileDB]: The CollectionFiles with the specified `gdrive_file_id` values, if such CollectionFiles exist in the database.
    """
    async with session:
        collection_query = select(CollectionFileDB).where(col(CollectionFileDB.gdrive_file_id).in_(gdrive_file_ids))
        collections = (await session.exec(collection_query)).all()
        return collections


async def list_collection_files(session: AsyncSession, imported: Optional[bool] = None) -> list[CollectionFileDB]:
    """Retrieves CollectionFiles meeting the specified criteria, ordered ascending by `CollectionFileDB.timestamp`.

    Args:
        session (AsyncSession): The database session to use.
        imported (bool, optional): If specified returns only CollectionFiles that have already been imported or not.

    Returns:
        list[CollectionFileDB]: A list of CollectionFiles meeting the criteria.
    """
    async with session:
        query = select(CollectionFileDB).order_by(asc(CollectionFileDB.timestamp))

        if imported is not None:
            query = query.where(is_(CollectionFileDB.imported, imported))

        results = await session.exec(query)
        return list(results.all())


async def get_latest_imported_gdrive_modified_date(session: AsyncSession) -> Optional[datetime]:
    async with session:
        query = select(CollectionFileDB).where(is_not(CollectionFileDB.imported, None)).order_by(desc(CollectionFileDB.gdrive_modified_date))
        result = await session.exec(query)
        collection_file = result.first()
        if collection_file:
            return collection_file.gdrive_modified_date
        return None


async def save_collection_file(session: AsyncSession, collection_file: CollectionFileDB) -> CollectionFileDB:
    """Inserts a CollectionFile into the database or updates an existing one.

    Args:
        session (AsyncSession): The database session to use.
        collection_file (CollectionFileDB): The CollectionFile to be saved.

    Returns:
        CollectionFileDB: The inserted or updated CollectionFile.
    """
    async with session:
        session.add(collection_file)
        await session.commit()
        await session.refresh(collection_file)
        return collection_file


async def save_collection_files(session: AsyncSession, collection_files: Iterable[CollectionFileDB]) -> list[CollectionFileDB]:
    """Inserts a CollectionFile into the database or updates an existing one.

    Args:
        session (AsyncSession): The database session to use.
        collection_files (Iterable[CollectionFileDB]): A collection of CollectionFiles to be saved.

    Returns:
        list[CollectionFileDB]: The inserted or updated CollectionFiles.
    """
    async with session:
        for collection_file in collection_files:
            session.add(collection_file)

        await session.commit()

        for collection_file in collection_files:
            await session.refresh(collection_file)

        return collection_files


__all__ = [
    get_collection_file_by_id.__name__,
    list_collection_files.__name__,
    list_collection_files_by_gdrive_file_ids.__name__,
    save_collection_file.__name__,
    save_collection_files.__name__,
]
