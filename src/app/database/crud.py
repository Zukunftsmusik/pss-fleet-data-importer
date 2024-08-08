from datetime import datetime
from typing import Iterable, Optional

from sqlalchemy.sql.operators import is_, is_not
from sqlmodel import asc, col, desc, select
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


async def get_collection_file_db(session: AsyncSession, gdrive_file_id: str, file_name: str, timestamp: datetime) -> CollectionFileDB:
    """Retrieves a CollectionFile by its `gdrive_file_id` from the database or creates a new one, if such a CollectionFile doesn't exist, yet. If the CollectionFile already exists in the database, it's not checked for equal file name or timestamp.

    Args:
        session (AsyncSession): The database session to use.
        gdrive_file_id (str): The `gdrive_file_id` of the CollectionFile to retrieve.
        file_name (str): The file name of the CollectionFile.
        timestamp (datetime): The timestamp of the CollectionFile.

    Returns:
        CollectionFileDB: A CollectionFile with the requested `gdrive_file_id`.
    """
    async with session:
        collection_file = await get_collection_file_by_gdrive_file_id(session, gdrive_file_id)
        if collection_file:
            return collection_file

        collection_file = CollectionFileDB(
            gdrive_file_id=gdrive_file_id,
            file_name=file_name,
            timestamp=timestamp,
        )
        collection_file = await save_collection_file(session, collection_file)
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


async def get_collection_files_by_gdrive_file_ids(session: AsyncSession, gdrive_file_ids: Iterable[str]) -> list[CollectionFileDB]:
    """Retrieves the CollectionFile with the given `gdrive_file_id`.

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
        list[CollectionFileDB]: A list of CollectionFiles meeting the criteria.
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


async def get_latest_downloaded_collection_file(session: AsyncSession) -> Optional[CollectionFileDB]:
    """Retrieves the CollectionFile with the most recent `timestamp`, which has already been downloaded.

    Args:
        session (AsyncSession): The database session to use.

    Returns:
        Optional[CollectionFileDB]: The CollectionFile with the most recent `timestamp`, which has already been downloaded, or `None` if the database is empty.
    """
    async with session:
        query = select(CollectionFileDB).where(is_not(CollectionFileDB.downloaded_at, None)).order_by(desc(CollectionFileDB.timestamp)).limit(1)

        result = await session.exec(query)
        return result.first()


async def get_latest_imported_collection_file(session: AsyncSession) -> Optional[CollectionFileDB]:
    """Retrieves the CollectionFile with the most recent `timestamp`, which has already been imported.

    Args:
        session (AsyncSession): The database session to use.

    Returns:
        Optional[CollectionFileDB]: The CollectionFile with the most recent `timestamp`, which has already been imported, or `None` if the database is empty.
    """
    async with session:
        query = select(CollectionFileDB).where(is_not(CollectionFileDB.imported_at, None)).order_by(desc(CollectionFileDB.timestamp)).limit(1)

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


async def insert_new_collection_files(session: AsyncSession, collection_files: Iterable[CollectionFileDB]) -> list[CollectionFileDB]:
    """From a collection of CollectionFiles, retrieve those from the database that already exist and insert those that don't exist, yet.

    Args:
        session (AsyncSession): The database session to use.
        collection_files (Iterable[CollectionFileDB]): A collection of CollectionFiles, already existing in the database or not.

    Returns:
        list[CollectionFileDB]: A list of CollectionFiles from the database. The order of the list elements may differ from the original order passed into the function.
    """
    async with session:
        existing_collection_files = await get_collection_files_by_gdrive_file_ids(
            session, [collection_file.gdrive_file_id for collection_file in collection_files]
        )
        existing_gdrive_file_ids = [collection_file.gdrive_file_id for collection_file in existing_collection_files]

        new_collection_files = [
            collection_file for collection_file in collection_files if collection_file.gdrive_file_id not in existing_gdrive_file_ids
        ]
        new_collection_files = await save_collection_files(session, new_collection_files)

        result = list(existing_collection_files)
        result.extend(new_collection_files)
        return result


__all__ = [
    delete_collection_file.__name__,
    get_collection_file.__name__,
    get_collection_file_by_gdrive_file_id.__name__,
    get_collection_file_by_name.__name__,
    get_collection_file_by_timestamp.__name__,
    get_collection_file_db.__name__,
    get_collection_files.__name__,
    get_collection_files_by_gdrive_file_ids.__name__,
    get_latest_collection_file.__name__,
    get_latest_downloaded_collection_file.__name__,
    get_latest_imported_collection_file.__name__,
    save_collection_file.__name__,
    save_collection_files.__name__,
]
