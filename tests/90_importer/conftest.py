import pytest

from src.app.database import crud


@pytest.fixture(scope="function")
def patch_crud_save_collection(monkeypatch: pytest.MonkeyPatch):
    async def mock_save_collection_file(_, collection_file):
        return collection_file

    monkeypatch.setattr(crud, crud.save_collection_file.__name__, mock_save_collection_file)
