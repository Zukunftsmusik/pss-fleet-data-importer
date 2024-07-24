from datetime import datetime
from typing import Optional

from sqlalchemy.sql.operators import is_, is_not
from sqlmodel import asc, desc, select
from sqlmodel.ext.asyncio.session import AsyncSession

from ..core import utils
from .models import CollectionFileDB


async def delete_collection_file(session: AsyncSession, collection_file_id: int) -> bool:
    """Attempts to delete the CollectionFile with the provided `collection_file_id`.

    Args:
        session (AsyncSession): The database session to use.
        collection_file_id (int): The `id` of the CollectionFile to delete.

    Returns:
        bool: Returns `True`, if such a CollectionFile exists and is deleted successfully. Returns `False`, if an error occured while deleting the CollectionFile.
    """
    async with session:
        collection_file = await get_collection_file(session, collection_file_id)
        try:
            await session.delete(collection_file)
            await session.commit()
            return True
        except Exception as e:
            print(e)
            return False


async def get_collection_file(session: AsyncSession, collection_file_id: int) -> Optional[CollectionFileDB]:
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


async def get_collection_file_by_gdrive_file_id(session: AsyncSession, gdrive_file_id: str) -> Optional[CollectionFileDB]:
    """Retrieves the CollectionFile with the given `gdrive_file_id`.

    Args:
        session (AsyncSession): The database session to use.
        gdrive_file_id (str): The `gdrive_file_id` of the CollectionFile to look for.

    Returns:
        Optional[CollectionFileDB]: The CollectionFile with the specified `gdrive_file_id` value, if such a CollectionFile exists in the database. Else, `None`.
    """
    async with session:
        collection_query = select(CollectionFileDB).where(CollectionFileDB.gdrive_file_id == gdrive_file_id)
        collection = (await session.exec(collection_query)).first()
        return collection


async def get_collection_file_by_name(session: AsyncSession, file_name: str) -> Optional[CollectionFileDB]:
    """Retrieves the CollectionFile with the given `file_name`.

    Args:
        session (AsyncSession): The database session to use.
        file_name (str): The `file_name` of the CollectionFile to look for.

    Returns:
        Optional[CollectionFileDB]: The CollectionFile with the specified `file_name` value, if such a CollectionFile exists in the database. Else, `None`.
    """
    async with session:
        collection_query = select(CollectionFileDB).where(CollectionFileDB.file_name == file_name)
        collection = (await session.exec(collection_query)).first()
        return collection


async def get_collection_file_by_timestamp(session: AsyncSession, timestamp: datetime) -> Optional[CollectionFileDB]:
    """Retrieves the CollectionFile with the given `timestamp` datetime.

    Args:
        session (AsyncSession): The database session to use.
        timestamp (datetime): The `timestamp` of the CollectionFile to look for.

    Returns:
        Optional[CollectionFileDB]: The CollectionFile with the specified `timestamp` value, if such a CollectionFile exists in the database. Else, `None`.
    """
    timestamp = utils.remove_timezone(timestamp)

    async with session:
        collection_query = select(CollectionFileDB).where(CollectionFileDB.timestamp == timestamp)
        collection = (await session.exec(collection_query)).first()
        return collection


async def get_collection_files(session: AsyncSession, downloaded: Optional[bool] = None, imported: Optional[bool] = None) -> list[CollectionFileDB]:
    """Retrieves CollectionFiles meeting the specified criteria, ordered ascending by `CollectionFileDB.timestamp`.

    Args:
        session (AsyncSession): The database session to use.
        downloaded (bool, optional): If specified returns only CollectionFiles that have already been downloaded or not.
        imported (bool, optional): If specified returns only CollectionFiles that have already been imported or not.

    Returns:
        list[CollectionFileDB]: A list of Collections without any Alliances or Users.
    """
    async with session:
        query = select(CollectionFileDB).order_by(asc(CollectionFileDB.timestamp))

        if downloaded is True:
            query = query.where(is_not(CollectionFileDB.downloaded_at, None))
        elif downloaded is False:
            query = query.where(is_(CollectionFileDB.downloaded_at, None))

        if imported is True:
            query = query.where(is_not(CollectionFileDB.imported_at, None))
        elif imported is False:
            query = query.where(is_(CollectionFileDB.imported_at, None))

        results = await session.exec(query)
        return list(results.all())


async def get_latest_collection_file(session: AsyncSession) -> Optional[CollectionFileDB]:
    """Retrieves the CollectionFile with the most recent `timestamp`.

    Args:
        session (AsyncSession): The database session to use.

    Returns:
        Optional[CollectionFileDB]: The CollectionFile with the most recent `timestamp` or `None` if the database is empty.
    """
    async with session:
        query = select(CollectionFileDB).order_by(desc(CollectionFileDB.timestamp)).limit(1)

        result = await session.exec(query)
        return result.first()


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


__all__ = [
    delete_collection_file.__name__,
    get_collection_files.__name__,
    save_collection_file.__name__,
]
