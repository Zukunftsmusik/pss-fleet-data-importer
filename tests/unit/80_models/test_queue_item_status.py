from datetime import datetime
from pathlib import Path

from src.app.core.gdrive import GDriveFile
from src.app.core.models.cancellation_token import CancellationToken
from src.app.core.models.status import StatusFlag
from src.app.database.models import CollectionFileDB
from src.app.models.queue_item import QueueItemStatus


def test_create(cancel_token: CancellationToken):
    queue_item_status = QueueItemStatus(cancel_token)

    assert queue_item_status.cancel_token == cancel_token
    assert isinstance(queue_item_status.downloaded, StatusFlag)
    assert queue_item_status.downloaded.value is False
    assert isinstance(queue_item_status.download_error, StatusFlag)
    assert queue_item_status.download_error.value is False
    assert isinstance(queue_item_status.download_timed_out, StatusFlag)
    assert queue_item_status.download_timed_out.value is False
    assert isinstance(queue_item_status.imported, StatusFlag)
    assert queue_item_status.imported.value is False
    assert isinstance(queue_item_status.import_error, StatusFlag)
    assert queue_item_status.import_error.value is False


def test_properties(cancel_token: CancellationToken):
    queue_item_status = QueueItemStatus(cancel_token)
    timestamp = datetime(2024, 8, 1)

    # writable properties
    queue_item_status.downloaded_at = timestamp
    assert queue_item_status.downloaded_at == timestamp
    queue_item_status.downloaded_at = None
    assert queue_item_status.downloaded_at is None

    queue_item_status.imported_at = timestamp
    assert queue_item_status.imported_at == timestamp
    queue_item_status.imported_at = None
    assert queue_item_status.imported_at is None
