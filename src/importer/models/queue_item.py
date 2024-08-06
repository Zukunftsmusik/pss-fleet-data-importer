from pathlib import Path
from threading import Lock

from cancel_token import CancellationToken
from pydrive2.files import GoogleDriveFile

from ..core import utils
from ..database import AsyncAutoRollbackSession, CollectionFileDB, Database, crud
from .collection_file_change import CollectionFileChange


class CollectionFileQueueItem:
    def __init__(
        self,
        item_no: int,
        gdrive_file: GoogleDriveFile,
        collection_file: CollectionFileDB,
        target_directory: Path,
        cancel_token: CancellationToken,
    ):
        self.cancel_token: CancellationToken = cancel_token
        self.item_no: int = item_no
        self.gdrive_file: GoogleDriveFile = gdrive_file
        self.__collection_file: CollectionFileDB = collection_file
        self.__collection_file_lock: Lock = Lock()
        self.__download_file_path: Path = None
        self.__download_file_path_lock: Lock = Lock()
        self.__target_directory_path: Path = Path(target_directory)
        self.__error_while_downloading: bool = False
        self.__error_while_downloading_lock: Lock = Lock()

    @property
    def collection_file(self) -> CollectionFileDB:
        with self.__collection_file_lock:
            return self.__collection_file

    @collection_file.setter
    def collection_file(self, value: CollectionFileDB):
        with self.__collection_file_lock:
            self.__collection_file = value

    @property
    def download_file_path(self) -> Path:
        with self.__download_file_path_lock:
            return self.__download_file_path

    @download_file_path.setter
    def download_file_path(self, value: Path):
        with self.__download_file_path_lock:
            self.__download_file_path = value

    @property
    def error_while_downloading(self) -> bool:
        with self.__error_while_downloading_lock:
            return self.__error_while_downloading

    @error_while_downloading.setter
    def error_while_downloading(self, value: bool) -> bool:
        with self.__error_while_downloading_lock:
            self.__error_while_downloading = value

    @property
    def gdrive_file_size(self) -> int:
        return int(self.gdrive_file["fileSize"])

    @property
    def gdrive_file_id(self) -> str:
        return self.gdrive_file["id"]

    @property
    def gdrive_file_name(self) -> str:
        return utils.get_gdrive_file_name(self.gdrive_file)

    @property
    def target_directory_path(self) -> Path:
        return self.__target_directory_path

    @property
    def target_file_path(self) -> Path:
        return self.target_directory_path.joinpath(self.gdrive_file_name)

    async def update_collection_file(self, database: Database, change: CollectionFileChange) -> CollectionFileDB:
        with self.__collection_file_lock:
            if change.downloaded_at:
                self.__collection_file.downloaded_at = change.downloaded_at

            if change.imported_at:
                self.__collection_file.imported_at = change.imported_at

            if not self.cancel_token.cancelled:
                async with AsyncAutoRollbackSession(database) as session:
                    self.__collection_file = await crud.save_collection_file(session, self.__collection_file)

            return self.__collection_file


__all__ = [
    CollectionFileQueueItem.__name__,
]
