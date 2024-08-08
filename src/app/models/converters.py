from pathlib import Path
from typing import Iterable

import dateutil
import dateutil.parser
from cancel_token import CancellationToken
from pydrive2.files import GoogleDriveFile

from ..core import utils
from ..database.models import CollectionFileDB
from .queue_item import CollectionFileQueueItem


class FromGdriveFile:
    @staticmethod
    def to_collection_file(gdrive_file: GoogleDriveFile) -> CollectionFileDB:
        file_name = utils.get_gdrive_file_name(gdrive_file)
        timestamp = utils.extract_timestamp_from_gdrive_file_name(file_name)

        collection_file = CollectionFileDB(
            gdrive_file_id=gdrive_file["id"],
            file_name=file_name,
            gdrive_modified_date=utils.remove_timezone(dateutil.parser.parse(gdrive_file["modifiedDate"])),
            timestamp=timestamp,
        )
        return collection_file

    @staticmethod
    def to_collection_files(gdrive_files: Iterable[GoogleDriveFile]) -> list[CollectionFileDB]:
        return [FromGdriveFile.to_collection_file(gdrive_file) for gdrive_file in gdrive_files]


class FromCollectionFileDB:
    @staticmethod
    def to_queue_items(
        gdrive_files: Iterable[GoogleDriveFile],
        collection_files: Iterable[CollectionFileDB],
        target_directory: Path,
        cancel_token: CancellationToken,
    ) -> list[CollectionFileQueueItem]:
        gdrive_files_by_id = {gdrive_file["id"]: gdrive_file for gdrive_file in gdrive_files}
        collection_files_by_gdrive_file_id = {collection_file.gdrive_file_id: collection_file for collection_file in collection_files}

        if set(gdrive_files_by_id.keys()) != set(collection_files_by_gdrive_file_id):
            raise ValueError

        result = [
            CollectionFileQueueItem(
                i + 1,
                gdrive_files_by_id[gdrive_file_id],
                collection_files_by_gdrive_file_id[gdrive_file_id],
                target_directory,
                cancel_token,
            )
            for i, gdrive_file_id in enumerate(gdrive_files_by_id.keys())
        ]
        return result
