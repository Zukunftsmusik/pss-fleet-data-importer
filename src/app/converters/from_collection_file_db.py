from pathlib import Path
from typing import Iterable

from ..core.gdrive import GDriveFile
from ..core.models.cancellation_token import CancellationToken
from ..database.models import CollectionFileDB
from ..models.queue_item import QueueItem


class FromCollectionFileDB:
    @staticmethod
    def to_queue_items(
        gdrive_files: Iterable[GDriveFile],
        collection_files: Iterable[CollectionFileDB],
        target_directory: Path,
        cancel_token: CancellationToken,
    ) -> list[QueueItem]:
        gdrive_files_by_id = {gdrive_file.id: gdrive_file for gdrive_file in gdrive_files}
        collection_files_by_gdrive_file_id = {collection_file.gdrive_file_id: collection_file for collection_file in collection_files}

        if set(gdrive_files_by_id.keys()) != set(collection_files_by_gdrive_file_id):
            raise ValueError

        result = []
        for item_no, gdrive_file_id in enumerate(gdrive_files_by_id.keys(), 1):
            gdrive_file = gdrive_files_by_id[gdrive_file_id]
            collection_file = collection_files_by_gdrive_file_id[gdrive_file_id]

            queue_item = QueueItem(
                item_no,
                gdrive_file,
                collection_file.collection_file_id,
                target_directory,
                cancel_token,
            )

            queue_item.status.downloaded.value = bool(collection_file.downloaded_at)
            queue_item.status.downloaded_at = collection_file.downloaded_at
            queue_item.status.imported.value = bool(collection_file.imported_at)
            queue_item.status.imported_at = collection_file.imported_at

            result.append(queue_item)

        return result
