from fake_classes import FakeUnitOfWork
from src.app.core.models.collection_file_change import CollectionFileChange
from src.app.database.models import CollectionFileDB
from src.app.importer.importer import update_database


async def test_commits(collection_file_db: CollectionFileDB):
    uow = FakeUnitOfWork()
    uow.collection_files.add(collection_file_db)

    change = CollectionFileChange(collection_file_id=collection_file_db.collection_file_id)

    await update_database(change, 1, uow)

    assert uow.committed is True


async def test_change_is_applied(collection_file_db: CollectionFileDB):
    uow = FakeUnitOfWork()
    uow.collection_files.add(collection_file_db)

    change = CollectionFileChange(collection_file_id=collection_file_db.collection_file_id, imported=True)
    await update_database(change, 1, uow)

    assert (await uow.collection_files.get_by_id(collection_file_db.collection_file_id)).imported is True

    change = CollectionFileChange(collection_file_id=collection_file_db.collection_file_id, imported=False, error=True)
    await update_database(change, 1, uow)

    assert (await uow.collection_files.get_by_id(collection_file_db.collection_file_id)).imported is False
    assert (await uow.collection_files.get_by_id(collection_file_db.collection_file_id)).error is True
