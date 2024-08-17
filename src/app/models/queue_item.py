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
        collection_file: CollectionFileDB,
        target_directory: Union[Path, str],
        cancel_token: CancellationToken,
    ):
        self.cancel_token: CancellationToken = cancel_token
        self.item_no: int = item_no
        self.gdrive_file: GDriveFile = gdrive_file
        self.__collection_file: CollectionFileDB = collection_file
        self.__collection_file_lock: Lock = Lock()
        self.__downloaded: bool = None
        self.__downloaded_lock: Lock = Lock()
        self.__target_directory_path: Path = Path(target_directory)
        self.__error_while_downloading: bool = False
        self.__error_while_downloading_lock: Lock = Lock()
        self.status = QueueItemStatus()

    @property
    def collection_file(self) -> CollectionFileDB:
        with self.__collection_file_lock:
            return self.__collection_file

    @collection_file.setter
    def collection_file(self, value: CollectionFileDB):
        with self.__collection_file_lock:
            self.__collection_file = value

    @property
    def downloaded(self) -> bool:
        with self.__downloaded_lock:
            return self.__downloaded

    @downloaded.setter
    def downloaded(self, value: bool):
        with self.__downloaded_lock:
            self.__downloaded = value

    @property
    def error_while_downloading(self) -> bool:
        with self.__error_while_downloading_lock:
            return self.__error_while_downloading

    @error_while_downloading.setter
    def error_while_downloading(self, value: bool) -> bool:
        with self.__error_while_downloading_lock:
            self.__error_while_downloading = value

    @property
    def target_directory_path(self) -> Path:
        return self.__target_directory_path

    @property
    def target_file_path(self) -> Path:
        return self.target_directory_path.joinpath(self.gdrive_file.name)

    async def update_collection_file(self, change: CollectionFileChange) -> CollectionFileDB:
        with self.__collection_file_lock:
            if change.downloaded_at:
                self.__collection_file.downloaded_at = change.downloaded_at

            if change.imported_at:
                self.__collection_file.imported_at = change.imported_at

            if change.download_error is not None:
                self.__collection_file.download_error = change.download_error

            if not self.cancel_token or not self.cancel_token.cancelled:
                async with DatabaseRepository.get_session() as session:
                    self.__collection_file = await crud.save_collection_file(session, self.__collection_file)

            return self.__collection_file


__all__ = [
    QueueItem.__name__,
]
