from pathlib import Path
from threading import Lock
from typing import Union

from ..core.gdrive import GDriveFile
from ..core.models.cancellation_token import CancellationToken
from ..core.models.collection_file_change import CollectionFileChange
from ..core.models.status import StatusFlag
from ..database import crud
from ..database.db_repository import DatabaseRepository
from ..database.models import CollectionFileDB


class QueueItemStatus:
    downloading = StatusFlag("downloading", False)
    downloaded = StatusFlag("downloaded", False)
    download_error = StatusFlag("download_error", False)
    importing = StatusFlag("importing", False)
    imported = StatusFlag("imported", False)
    import_error = StatusFlag("import_error", False)


class QueueItem:
    def __init__(
        self,
        item_no: int,
        gdrive_file: GDriveFile,
        collection_file_id: int,
        target_directory: Union[Path, str],
        cancel_token: CancellationToken,
    ):
        self.cancel_token: CancellationToken = cancel_token
        self.item_no: int = item_no
        self.gdrive_file: GDriveFile = gdrive_file
        self.collection_file_id: CollectionFileDB = collection_file_id
        self.target_directory_path: Path = Path(target_directory)
        self.status = QueueItemStatus()

    @property
    def target_file_path(self) -> Path:
        return self.target_directory_path.joinpath(self.gdrive_file.name)


__all__ = [
    # Classes
    QueueItem.__name__,
    QueueItemStatus.__name__,
]
