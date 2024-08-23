from datetime import datetime
from typing import Optional

import pytest

from fake_classes import FakeUnitOfWork, create_fake_collection_file, create_fake_gdrive_file
from src.app.core import utils
from src.app.importer.importer import get_updated_modified_after


test_cases_no_files_imported_yet = [
    # modified_after
    pytest.param(None, id="none"),
    pytest.param(datetime(2024, 1, 1), id="2024-01-01"),
]
"""modified_after: Optional[datetime]"""


@pytest.mark.parametrize(["modified_after"], test_cases_no_files_imported_yet)
async def test_return_input_if_no_file_imported_yet(modified_after: Optional[datetime]):
    uow = FakeUnitOfWork()

    result_1 = await get_updated_modified_after(modified_after, uow=uow)

    uow.collection_files.add(create_fake_collection_file())

    result_2 = await get_updated_modified_after(modified_after, uow=uow)

    if modified_after is None:
        assert result_1 is None
        assert result_2 is None
    else:
        assert result_1 == modified_after
        assert result_2 == modified_after


async def test_return_next_full_hour_after_last_imported_modified_date():
    uow = FakeUnitOfWork()

    collection_file = create_fake_collection_file()
    collection_file.imported = True
    uow.collection_files.add(collection_file)

    result = await get_updated_modified_after(uow=uow)

    assert result == utils.get_next_full_hour(collection_file.gdrive_modified_date)


async def test_return_newer_updated_modified_date():
    uow = FakeUnitOfWork()
    modified_after = datetime(2024, 2, 1)

    gdrive_file = create_fake_gdrive_file(modified_date_after=datetime(2024, 3, 1))
    collection_file = create_fake_collection_file(gdrive_file=gdrive_file)
    collection_file.imported = True
    uow.collection_files.add(collection_file)

    result = await get_updated_modified_after(modified_after=modified_after, uow=uow)

    assert gdrive_file.modified_date > modified_after
    assert result == utils.get_next_full_hour(gdrive_file.modified_date)
    assert result > modified_after

    uow = FakeUnitOfWork()

    gdrive_file = create_fake_gdrive_file(modified_date_before=datetime(2024, 1, 1))
    collection_file = create_fake_collection_file(gdrive_file=gdrive_file)
    collection_file.imported = True

    result = await get_updated_modified_after(modified_after=modified_after, uow=uow)

    assert gdrive_file.modified_date < modified_after
    assert result > gdrive_file.modified_date
    assert result == modified_after
