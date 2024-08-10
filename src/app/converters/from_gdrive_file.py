from typing import Iterable

import dateutil
import dateutil.parser
from pydrive2.files import GoogleDriveFile

from ..core import utils
from ..database.models import CollectionFileDB


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
