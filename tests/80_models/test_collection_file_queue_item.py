from pathlib import Path

from src.app.core.gdrive import GDriveFile
from src.app.core.models.cancellation_token import CancellationToken
from src.app.database.models import CollectionFileDB
from src.app.models.queue_item import QueueItem


def test_create(gdrive_file: GDriveFile, collection_file_db: CollectionFileDB, cancel_token: CancellationToken):
    queue_item = QueueItem(1, gdrive_file, collection_file_db, "/dev/null/", cancel_token)

    assert queue_item.item_no == 1
    assert queue_item.gdrive_file == gdrive_file
    assert queue_item.collection_file == collection_file_db
    assert isinstance(queue_item.target_directory_path, Path)
    assert queue_item.cancel_token == cancel_token
    assert queue_item.downloaded is None
    assert queue_item.error_while_downloading is False
    assert queue_item.target_directory_path == Path("/dev/null")


def test_properties(
    queue_item: QueueItem,
    gdrive_file: GDriveFile,
    collection_file_db: CollectionFileDB,
):
    # read-only properties
    assert queue_item.target_file_path == Path(f"/dev/null/{gdrive_file.name}")

    # setters
    queue_item.collection_file = None
    assert queue_item.collection_file is None
    queue_item.collection_file = collection_file_db
    assert queue_item.collection_file == collection_file_db

    queue_item.downloaded = True
    assert queue_item.downloaded == True
    queue_item.downloaded = False
    assert queue_item.downloaded == False
    queue_item.downloaded = None
    assert queue_item.downloaded is None

    queue_item.error_while_downloading = True
    assert queue_item.error_while_downloading is True
    queue_item.error_while_downloading = False
    assert queue_item.error_while_downloading is False
    queue_item.error_while_downloading = None
    assert queue_item.error_while_downloading is None
