import datetime
from threading import Lock

from . import CollectionFileBase


class CollectionFileImport(CollectionFileBase):
    def __init__(self, collection_file: CollectionFileBase):
        self.__collection_file = collection_file

        self.__downloaded: bool = False
        self.__downloaded_lock: Lock = Lock()

        self.__imported: bool = False
        self.__imported_lock: Lock = Lock()

    @property
    def downloaded(self) -> bool:
        with self.__downloaded_lock:
            return self.__downloaded

    @property
    def file_name(self) -> str:
        return self.__collection_file.file_name

    @property
    def gdrive_file_id(self) -> str:
        return self.__collection_file.gdrive_file_id

    @property
    def imported(self) -> bool:
        with self.__imported_lock:
            return self.__imported

    @property
    def timestamp(self) -> datetime:
        return self.__collection_file.timestamp

    @downloaded.setter
    def set_downloaded(self, downloaded: bool) -> bool:
        with self.__downloaded_lock:
            self.__downloaded = downloaded

    @imported.setter
    def set_imported(self, imported: bool) -> bool:
        with self.__imported_lock:
            self.__imported = imported


__all__ = [
    CollectionFileImport.__name__,
]
