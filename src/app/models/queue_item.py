from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Optional, Union

from ..core.gdrive import GDriveFile
from ..core.models.cancellation_token import CancellationToken
from ..core.models.status import StatusFlag
from ..database.models import CollectionFileDB


class QueueItemStatus:
    def __init__(self, cancel_token: CancellationToken):
        self.downloaded = StatusFlag("downloaded", False)
        self.download_error = StatusFlag("download_error", False)
        self.download_timed_out = StatusFlag("download_timed_out", False)
        self.imported = StatusFlag("imported", False)
        self.import_error = StatusFlag("import_error", False)
        self.__downloaded_at: datetime = None
        self.__downloaded_at_lock = Lock()
        self.__imported_at: datetime = None
        self.__imported_at_lock = Lock()
        self.cancel_token = cancel_token

    @property
    def done(self) -> bool:
        return self.imported.value or self.import_error.value or self.download_error.value

    @property
    def downloaded_at(self) -> Optional[datetime]:
        with self.__downloaded_at_lock:
            return self.__downloaded_at

    @downloaded_at.setter
    def downloaded_at(self, value: datetime):
        with self.__downloaded_at_lock:
            self.__downloaded_at = value

    @property
    def imported_at(self) -> Optional[datetime]:
        with self.__imported_at_lock:
            return self.__imported_at

    @imported_at.setter
    def imported_at(self, value: datetime):
        with self.__imported_at_lock:
            self.__imported_at = value


class QueueItem:
    def __init__(
        self,
        item_no: int,
        gdrive_file: GDriveFile,
        collection_file_id: int,
        target_directory: Union[Path, str],
        cancel_token: CancellationToken,
    ):
        self.item_no: int = item_no
        self.gdrive_file: GDriveFile = gdrive_file
        self.collection_file_id: CollectionFileDB = collection_file_id
        self.target_directory_path: Path = Path(target_directory)
        self.status = QueueItemStatus(cancel_token)

    @property
    def target_file_path(self) -> Path:
        return self.target_directory_path.joinpath(self.gdrive_file.name)


__all__ = [
    # Classes
    QueueItem.__name__,
    QueueItemStatus.__name__,
]
