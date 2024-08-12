from typing import Iterable

from ..core import utils
from ..core.gdrive import GDriveFile
from ..database.models import CollectionFileDB


class FromGdriveFile:
    @staticmethod
    def to_collection_file(gdrive_file: GDriveFile) -> CollectionFileDB:
        timestamp = utils.extract_timestamp_from_gdrive_file_name(gdrive_file.name)

        collection_file = CollectionFileDB(
            gdrive_file_id=gdrive_file.id,
            file_name=gdrive_file.name,
            gdrive_modified_date=utils.remove_timezone(gdrive_file.modified_date),
            timestamp=timestamp,
        )
        return collection_file

    @staticmethod
    def to_collection_files(gdrive_files: Iterable[GDriveFile]) -> list[CollectionFileDB]:
        return [FromGdriveFile.to_collection_file(gdrive_file) for gdrive_file in gdrive_files]
