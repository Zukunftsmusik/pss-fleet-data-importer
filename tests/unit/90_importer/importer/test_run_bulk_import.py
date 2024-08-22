from datetime import datetime

import pytest
from pydrive2.files import ApiRequestError

from fake_classes import FakeImporter, create_fake_gdrive_files
from src.app.database.unit_of_work import SqlModelUnitOfWork
from src.app.importer.importer import create_collection_files


@pytest.mark.usefixtures("patch_get_config_return_fake", "reset_database_after_test")
async def test_happy_path_fresh_database(fake_importer: FakeImporter):
    create_n_files = 10
    for gdrive_file in create_fake_gdrive_files(create_n_files):
        fake_importer.gdrive_client.files.append(gdrive_file)

    await fake_importer.run_bulk_import()

    uow = SqlModelUnitOfWork()
    async with uow:
        collection_files = await uow.collection_files.list_files()

    assert len(collection_files) == create_n_files
    assert all((collection_file.imported for collection_file in collection_files))


@pytest.mark.usefixtures("patch_get_config_return_fake", "reset_database_after_test")
async def test_happy_path_with_files_already_imported(fake_importer: FakeImporter):
    create_n_old_files = 5
    create_n_new_files = 5
    modified_date = datetime(2023, 1, 1)

    old_fake_gdrive_files = create_fake_gdrive_files(create_n_old_files, modified_date_before=modified_date)
    new_fake_gdrive_files = create_fake_gdrive_files(create_n_new_files, modified_date_after=modified_date)

    fake_importer.gdrive_client.files = old_fake_gdrive_files + new_fake_gdrive_files

    old_collection_files = create_collection_files(old_fake_gdrive_files)
    uow = SqlModelUnitOfWork()
    async with uow:
        for collection_file in old_collection_files:
            collection_file.imported = True
            uow.collection_files.add(collection_file)
        await uow.commit()

    await fake_importer.run_bulk_import()

    uow = SqlModelUnitOfWork()
    async with uow:
        collection_files = await uow.collection_files.list_files()

    assert len(collection_files) == create_n_old_files + create_n_new_files
    assert all((collection_file.imported for collection_file in collection_files))


@pytest.mark.usefixtures("patch_get_config_return_fake", "reset_database_after_test")
async def test_happy_path_with_files_uploaded_after_first_import(fake_importer: FakeImporter):
    create_n_old_files = 5
    create_n_new_files = 5
    modified_date = datetime(2023, 1, 1)

    old_fake_gdrive_files = create_fake_gdrive_files(create_n_old_files, modified_date_before=modified_date)
    new_fake_gdrive_files = create_fake_gdrive_files(create_n_new_files, modified_date_after=modified_date)

    fake_importer.gdrive_client.files = old_fake_gdrive_files

    await fake_importer.run_bulk_import()

    uow = SqlModelUnitOfWork()
    async with uow:
        collection_files = await uow.collection_files.list_files()

    assert len(collection_files) == create_n_old_files
    assert all((collection_file.imported for collection_file in collection_files))

    fake_importer.gdrive_client.files += new_fake_gdrive_files

    await fake_importer.run_bulk_import()

    uow = SqlModelUnitOfWork()
    async with uow:
        collection_files = await uow.collection_files.list_files()

    assert len(collection_files) == create_n_old_files + create_n_new_files
    assert all((collection_file.imported for collection_file in collection_files))


@pytest.mark.usefixtures("patch_get_config_return_fake", "reset_database_after_test", "patch_sleep")
async def test_happy_path_with_files_not_downloaded_from_gdrive(fake_importer: FakeImporter, api_request_error: ApiRequestError):
    create_n_ok_files = 8
    create_n_broken_files = 2

    ok_fake_gdrive_files = create_fake_gdrive_files(create_n_ok_files)
    broken_fake_gdrive_files = create_fake_gdrive_files(create_n_broken_files, get_content_exception=api_request_error)

    fake_importer.gdrive_client.files = ok_fake_gdrive_files + broken_fake_gdrive_files

    await fake_importer.run_bulk_import()

    uow = SqlModelUnitOfWork()
    async with uow:
        collection_files = await uow.collection_files.list_files()

    assert len(collection_files) == create_n_ok_files + create_n_broken_files
    assert len([collection_file for collection_file in collection_files if collection_file.imported]) == create_n_ok_files
    assert len([collection_file for collection_file in collection_files if collection_file.error]) == create_n_broken_files
