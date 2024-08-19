from pathlib import Path

from src.app.core.gdrive import GDriveFile
from src.app.core.models.cancellation_token import CancellationToken
from src.app.database.models import CollectionFileDB
from src.app.models.queue_item import QueueItem


def test_create(gdrive_file: GDriveFile, collection_file_db: CollectionFileDB, cancel_token: CancellationToken):
    queue_item = QueueItem(1, gdrive_file, collection_file_db.collection_file_id, "/dev/null/", cancel_token)

    assert queue_item.item_no == 1
    assert queue_item.gdrive_file == gdrive_file
    assert queue_item.collection_file_id == collection_file_db.collection_file_id
    assert isinstance(queue_item.target_directory_path, Path)
    assert queue_item.cancel_token == cancel_token
    assert queue_item.status.downloaded.value is False
    assert queue_item.status.download_error.value is False
    assert queue_item.status.imported.value is False
    assert queue_item.status.import_error.value is False
    assert queue_item.target_directory_path == Path("/dev/null")


def test_properties(
    queue_item: QueueItem,
    gdrive_file: GDriveFile,
):
    # read-only properties
    assert queue_item.target_file_path == Path(f"/dev/null/{gdrive_file.name}")
