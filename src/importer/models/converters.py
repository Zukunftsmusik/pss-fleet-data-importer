from pathlib import Path
from typing import Iterable

from pydrive2.files import GoogleDriveFile

from ..core import utils
from ..database.db import Database
from ..database.models import CollectionFileDB
from ..models.queue_item import CollectionFileQueueItem


class FromGdriveFile:
    @staticmethod
    def to_collection_file(gdrive_file: GoogleDriveFile) -> CollectionFileDB:
        file_name = utils.get_gdrive_file_name(gdrive_file)
        timestamp = utils.extract_timestamp_from_gdrive_file_name(file_name)

        collection_file = CollectionFileDB(
            gdrive_file_id=gdrive_file["id"],
            file_name=file_name,
            timestamp=timestamp,
        )
        return collection_file

    @staticmethod
    def to_collection_files(gdrive_files: Iterable[GoogleDriveFile]) -> list[CollectionFileDB]:
        return [FromGdriveFile.to_collection_file(gdrive_file) for gdrive_file in gdrive_files]


class FromCollectionFileDB:
    @staticmethod
    def to_queue_items(
        gdrive_files: Iterable[GoogleDriveFile], collection_files: Iterable[CollectionFileDB], target_directory: Path, database: Database
    ) -> list[CollectionFileQueueItem]:
        gdrive_files_by_id = {gdrive_file["id"]: gdrive_file for gdrive_file in gdrive_files}
        collection_files_by_gdrive_file_id = {collection_file.gdrive_file_id: collection_file for collection_file in collection_files}

        if set(gdrive_files_by_id.keys()) != set(collection_files_by_gdrive_file_id):
            raise ValueError

        result = [
            CollectionFileQueueItem(
                gdrive_files_by_id[gdrive_file_id], collection_files_by_gdrive_file_id[gdrive_file_id], target_directory, database
            )
            for gdrive_file_id in gdrive_files_by_id.keys()
        ]
        return result
