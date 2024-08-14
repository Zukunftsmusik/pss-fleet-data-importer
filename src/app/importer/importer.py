import asyncio
import queue
import threading
from datetime import datetime, timedelta
from typing import Iterable, Optional

from httpx import ConnectError
from pss_fleet_data import PssFleetDataClient

from ..converters import FromCollectionFileDB, FromGdriveFile
from ..core import utils
from ..core.config import Config
from ..core.gdrive import GDriveFile, GoogleDriveClient
from ..core.models.filesystem import FileSystem
from ..database import DatabaseRepository, crud
from ..database.models import CollectionFileDB
from ..log.log_importer import importer as log
from ..models import ImportStatus, QueueItem
from . import database_worker, download_worker, import_worker


class Importer:
    def __init__(
        self,
        config: Config,
        gdrive_client: GoogleDriveClient,
        pss_fleet_data_client: PssFleetDataClient,
    ):
        self.config: Config = config
        self.gdrive_client: GoogleDriveClient = gdrive_client
        self.fleet_data_client: PssFleetDataClient = pss_fleet_data_client

        self.status = ImportStatus()

        self.import_queue: queue.Queue = queue.Queue()
        self.database_queue: queue.Queue = queue.Queue()

    def cancel_workers(self):
        log.workers_cancel()
        self.status.cancel_token.cancel()

    async def check_api_server_connection(self) -> bool:
        try:
            await self.fleet_data_client.ping()
            return True
        except ConnectError:
            return False

    async def run_import_loop(self, modified_after: Optional[datetime] = None, modified_before: Optional[datetime] = None):
        cancel_message = "Import cancelled. Exiting import loop."
        while True:
            if self.status.cancel_token.log_if_cancelled(cancel_message):
                break

            after = modified_after
            if not modified_after:
                async with DatabaseRepository.get_session() as session:
                    earliest_not_imported_modified_date = await crud.get_earliest_gdrive_modified_date(session)
                    after = utils.get_next_full_hour(earliest_not_imported_modified_date) if earliest_not_imported_modified_date else None

            if self.status.cancel_token.log_if_cancelled(cancel_message):
                break

            did_import = await self.run_bulk_import(modified_after=after, modified_before=modified_before)

            if self.status.cancel_token.log_if_cancelled(cancel_message):
                break

            if did_import:
                continue

            await wait_for_import()

    async def run_bulk_import(self, modified_after: Optional[datetime] = None, modified_before: Optional[datetime] = None) -> bool:
        start = utils.get_now()
        print(f"### Starting bulk import at: {start.isoformat()}")

        log.bulk_import_start(modified_after, modified_before)

        log.download_gdrive_file_list_params(modified_after=modified_after, modified_before=modified_before)
        gdrive_files = get_gdrive_file_list(self.gdrive_client, modified_after=modified_after, modified_before=modified_before)

        log.download_gdrive_file_list_length(len(gdrive_files))
        if not gdrive_files:
            return False

        collection_files = create_collection_files(gdrive_files)
        await insert_new_collection_files(collection_files)

        log.queue_items_create()
        queue_items = FromCollectionFileDB.to_queue_items(gdrive_files, collection_files, self.config.temp_download_folder, self.status.cancel_token)

        log.download_folder_create(self.config.temp_download_folder)
        filesystem = FileSystem()
        filesystem.mkdir(self.config.temp_download_folder, create_parents=True, exist_ok=True)

        log.downloads_imports_count(queue_items)

        worker_threads = self.create_worker_threads(queue_items, filesystem=filesystem)

        for thread in worker_threads:
            thread.start()

        for thread in worker_threads:
            thread.join()

        end = utils.get_now()
        log.bulk_import_finish(queue_items, modified_after, modified_before)
        print(f"### Finished bulk import of {len(queue_items)} files at: {end.isoformat()} (after: {end - start})")

        return True

    def create_worker_threads(self, queue_items: Iterable[QueueItem], filesystem: FileSystem = FileSystem()) -> list[threading.Thread]:
        worker_threads = [
            threading.Thread(
                target=download_worker.worker,
                name="Download worker",
                args=[
                    queue_items,
                    self.gdrive_client,
                    self.config.download_thread_pool_size,
                    self.database_queue,
                    self.import_queue,
                    self.status.bulk_download_running,
                    60.0,
                    self.status.download_worker_timed_out,
                    self.status.cancel_token,
                    self.config.debug_mode,
                    filesystem,
                ],
                daemon=True,
            ),
            utils.create_async_thread(
                import_worker.worker,
                name="Import worker",
                args=(
                    self.import_queue,
                    self.database_queue,
                    self.fleet_data_client,
                    self.status.bulk_import_running,
                    self.status.cancel_token,
                    1,
                    self.config.keep_downloaded_files,
                    filesystem,
                ),
                daemon=True,
            ),
            utils.create_async_thread(
                database_worker.worker,
                name="Database worker",
                args=(
                    self.database_queue,
                    self.status.bulk_database_running,
                    self.status.cancel_token,
                    2,
                ),
                daemon=True,
            ),
        ]

        return worker_threads


def create_collection_files(gdrive_files: Iterable[GDriveFile]) -> list[CollectionFileDB]:
    collection_files = FromGdriveFile.to_collection_files(gdrive_files)
    collection_files.sort(key=lambda file: file.file_name.replace("-", "_"))  # There're files where some underscores are hyphens.
    return collection_files


def get_gdrive_file_list(
    gdrive_client: GoogleDriveClient,
    modified_after: Optional[datetime] = None,
    modified_before: Optional[datetime] = None,
) -> list[GDriveFile]:
    log.download_gdrive_file_list_start()

    with log.download_gdrive_file_list_duration():
        gdrive_files = list(gdrive_client.list_files_by_modified_date(modified_after, modified_before))

    return gdrive_files


async def insert_new_collection_files(collection_files: Iterable[CollectionFileDB]):
    log.database_entries_create()
    async with DatabaseRepository.get_session() as session:
        existing_collection_files = await crud.get_collection_files_by_gdrive_file_ids(
            session, [collection_file.gdrive_file_id for collection_file in collection_files]
        )
        existing_gdrive_file_ids = [collection_file.gdrive_file_id for collection_file in existing_collection_files]

        new_collection_files = [
            collection_file for collection_file in collection_files if collection_file.gdrive_file_id not in existing_gdrive_file_ids
        ]
        new_collection_files = await crud.save_collection_files(session, new_collection_files)

        result = list(existing_collection_files)
        result.extend(new_collection_files)
        return result


async def wait_for_import():
    now = utils.get_now()
    wait_until = utils.get_next_full_hour(now) + timedelta(minutes=1)
    wait_for_seconds = (wait_until - utils.get_now()).total_seconds()
    log.wait_for_import(wait_for_seconds, wait_until)
    await asyncio.sleep(wait_for_seconds)
