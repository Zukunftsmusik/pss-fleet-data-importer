from pathlib import Path
from typing import Iterable

from ..core.gdrive import GDriveFile
from ..core.models.cancellation_token import CancellationToken
from ..database.models import CollectionFileDB
from ..models.queue_item import CollectionFileQueueItem


class FromCollectionFileDB:
    @staticmethod
    def to_queue_items(
        gdrive_files: Iterable[GDriveFile],
        collection_files: Iterable[CollectionFileDB],
        target_directory: Path,
        cancel_token: CancellationToken,
    ) -> list[CollectionFileQueueItem]:
        gdrive_files_by_id = {gdrive_file.id: gdrive_file for gdrive_file in gdrive_files}
        collection_files_by_gdrive_file_id = {collection_file.gdrive_file_id: collection_file for collection_file in collection_files}

        if set(gdrive_files_by_id.keys()) != set(collection_files_by_gdrive_file_id):
            raise ValueError

        result = [
            CollectionFileQueueItem(
                item_no,
                gdrive_files_by_id[gdrive_file_id],
                collection_files_by_gdrive_file_id[gdrive_file_id],
                target_directory,
                cancel_token,
            )
            for item_no, gdrive_file_id in enumerate(gdrive_files_by_id.keys(), 1)
        ]
        return result
