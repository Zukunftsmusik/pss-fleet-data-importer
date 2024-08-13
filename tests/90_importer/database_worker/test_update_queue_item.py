from datetime import datetime
from typing import Optional

import pytest

from src.app.core.models.collection_file_change import CollectionFileChange
from src.app.importer.database_worker import update_queue_item
from src.app.models.queue_item import CollectionFileQueueItem


DOWNLOADED_AT = datetime(2024, 8, 1)
IMPORTED_AT = datetime(2024, 8, 1, 0, 1)


test_cases_updated = [
    # downloaded_at, imported_at, download_error
    pytest.param(None, None, None, id="none_none_none"),
    pytest.param(DOWNLOADED_AT, None, None, id="downloaded_none_none"),
    pytest.param(None, IMPORTED_AT, None, id="none_imported_none"),
    pytest.param(None, None, True, id="none_none_true"),
    pytest.param(None, None, False, id="none_none_false"),
    pytest.param(DOWNLOADED_AT, IMPORTED_AT, None, id="downloaded_imported_none"),
    pytest.param(DOWNLOADED_AT, None, True, id="downloaded_none_true"),
    pytest.param(DOWNLOADED_AT, None, False, id="downloaded_none_false"),
    pytest.param(None, IMPORTED_AT, True, id="none_imported_true"),
    pytest.param(None, IMPORTED_AT, False, id="none_imported_false"),
    pytest.param(DOWNLOADED_AT, IMPORTED_AT, True, id="downloaded_imported_true"),
    pytest.param(DOWNLOADED_AT, IMPORTED_AT, False, id="downloaded_imported_false"),
]
"""downloaded_at: Optional[datetime], imported_at: Optional[datetime], download_error: Optional[bool]"""


@pytest.mark.usefixtures("patch_crud_save_collection")
@pytest.mark.parametrize(["downloaded_at", "imported_at", "download_error"], test_cases_updated)
async def test_queue_item_is_updated(
    queue_item: CollectionFileQueueItem,
    downloaded_at: Optional[datetime],
    imported_at: Optional[datetime],
    download_error: Optional[bool],
):
    change = CollectionFileChange(downloaded_at=downloaded_at, imported_at=imported_at, download_error=download_error)

    downloaded_at_before = queue_item.collection_file.downloaded_at
    imported_at_before = queue_item.collection_file.imported_at
    download_error_before = queue_item.collection_file.download_error

    await update_queue_item(queue_item, change)

    assert queue_item.collection_file.downloaded_at == downloaded_at
    assert queue_item.collection_file.imported_at == imported_at
    assert queue_item.collection_file.download_error == download_error

    if downloaded_at:
        assert queue_item.collection_file.downloaded_at != downloaded_at_before
    if imported_at:
        assert queue_item.collection_file.imported_at != imported_at_before
    if download_error is not None:
        assert queue_item.collection_file.download_error != download_error_before
