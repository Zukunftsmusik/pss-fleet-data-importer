import pytest

from src.app.database import crud
from src.app.models.queue_item import CollectionFileQueueItem


@pytest.fixture(scope="function")
def patch_crud_save_collection(monkeypatch: pytest.MonkeyPatch):
    async def mock_save_collection_file(_, collection_file):
        return collection_file

    monkeypatch.setattr(crud, crud.save_collection_file.__name__, mock_save_collection_file)


@pytest.fixture(scope="function")
def queue_item(gdrive_file, collection_file_db) -> CollectionFileQueueItem:
    return CollectionFileQueueItem(1, gdrive_file, collection_file_db, "/dev/null", None)
