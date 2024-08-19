from fake_classes import FakeUnitOfWork, create_fake_collection_file
from src.app.importer.importer import insert_new_collection_files


async def test_insert_new_collection_files_commits():
    uow = FakeUnitOfWork()

    await insert_new_collection_files([], uow=uow)

    assert uow.committed is True


async def test_new_files_added():
    uow = FakeUnitOfWork()
    existing_files = [create_fake_collection_file(collection_file_id=i) for i in range(1, 6)]
    new_files = [create_fake_collection_file() for _ in range(1, 6)]

    for file in existing_files:
        uow.collection_files.add(file)

    collection_files = await insert_new_collection_files(new_files + existing_files, uow=uow)

    assert len(collection_files) == len(existing_files) + len(new_files)

    for file in existing_files:
        assert file in collection_files

    for file in new_files:
        assert file not in collection_files  # These got their collection_file_id updated
