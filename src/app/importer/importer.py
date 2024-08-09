import asyncio
import logging
import queue
import threading
from datetime import datetime, timedelta
from typing import Iterable, Optional

from httpx import ConnectError
from pss_fleet_data import PssFleetDataClient
from pydrive2.files import GoogleDriveFile

from ..core import utils, wrapper
from ..core.config import Config
from ..core.gdrive import GoogleDriveClient
from ..database import AsyncAutoRollbackSession, Database, crud
from ..models.converters import FromCollectionFileDB, FromGdriveFile
from ..models.queue_item import CollectionFileQueueItem
from ..models.status import ImportStatus
from . import database_worker, download_worker, import_worker, log


class Importer:
    def __init__(
        self,
        config: Config,
        database: Database,
        gdrive_client: GoogleDriveClient,
        pss_fleet_data_client: PssFleetDataClient,
    ):
        self.config: Config = config
        self.database: Database = database
        self.gdrive_client: GoogleDriveClient = gdrive_client
        self.fleet_data_client: PssFleetDataClient = pss_fleet_data_client

        self.logger: logging.Logger = self.config.logger.getChild(Importer.__name__)
        self.status = ImportStatus()

        self.import_queue: queue.Queue = queue.Queue()
        self.database_queue: queue.Queue = queue.Queue()

    def cancel_workers(self):
        self.logger.warn("Cancelling workers.")
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
            if self.status.cancel_token.log_if_cancelled(self.logger, cancel_message):
                break

            after = modified_after
            if not modified_after:
                async with AsyncAutoRollbackSession(self.database) as session:
                    earliest_not_imported_modified_date = await crud.get_earliest_gdrive_modified_date(session)
                    after = utils.get_next_full_hour(earliest_not_imported_modified_date) if earliest_not_imported_modified_date else None

            if self.status.cancel_token.log_if_cancelled(self.logger, cancel_message):
                break

            did_import = await self.run_bulk_import(modified_after=after, modified_before=modified_before)

            if self.status.cancel_token.log_if_cancelled(self.logger, cancel_message):
                break

            if did_import:
                continue

            now = utils.get_now()
            wait_until = utils.get_next_full_hour(now) + timedelta(minutes=1)
            wait_for_seconds = (wait_until - now).total_seconds()
            self.logger.info("Waiting for %.2f seconds until next import run at %s.", wait_for_seconds, wait_until.isoformat())
            await asyncio.sleep(wait_for_seconds)

    async def run_bulk_import(self, modified_after: Optional[datetime] = None, modified_before: Optional[datetime] = None) -> bool:
        start = utils.get_now()
        print(f"### Starting bulk import at: {start.isoformat()}")

        log.bulk_import_start(self.logger, modified_after, modified_before)

        self.logger.debug("Downloading Google Drive file list.")
        gdrive_files = self.gdrive_client.list_files_by_modified_date(modified_after, modified_before)
        gdrive_files = wrapper.debug_log_running_time(self.logger, "Downloading file list")(list, gdrive_files)

        if not gdrive_files:
            self.logger.info("No new files found to be imported.")
            return False

        self.logger.info(f"Found {len(gdrive_files)} new gdrive files to be imported.")

        collection_files = [FromGdriveFile.to_collection_file(gdrive_file) for gdrive_file in gdrive_files]
        collection_files.sort(key=lambda file: file.file_name.replace("-", "_"))  # There're files where some underscores are hyphens.

        self.logger.debug("Creating database entries.")
        async with AsyncAutoRollbackSession(self.database) as session:
            collection_files = await crud.insert_new_collection_files(session, collection_files)

        self.logger.debug("Creating queue items.")
        queue_items = FromCollectionFileDB.to_queue_items(gdrive_files, collection_files, self.config.temp_download_folder, self.status.cancel_token)

        self.logger.debug("Ensuring that download path '%s' exists.", self.config.temp_download_folder)
        self.config.temp_download_folder.mkdir(parents=True, exist_ok=True)

        log.downloads_imports(self.logger, queue_items)

        worker_threads = self.create_worker_threads(queue_items)

        for thread in worker_threads:
            thread.start()

        for thread in worker_threads:
            thread.join()

        end = utils.get_now()
        log.bulk_import_finish(self.logger, queue_items, modified_after, modified_before)
        print(f"### Finished bulk import of {len(queue_items)} files at: {end.isoformat()} (after: {end - start})")

        return True

    def create_worker_threads(self, queue_items: Iterable[CollectionFileQueueItem]) -> list[threading.Thread]:
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
                    self.logger,
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
                    self.logger,
                    self.status.cancel_token,
                    1,
                    self.config.keep_downloaded_files,
                ),
                daemon=True,
            ),
            utils.create_async_thread(
                database_worker.worker,
                name="Database worker",
                args=(
                    self.database,
                    self.database_queue,
                    self.logger,
                    self.status.bulk_database_running,
                    self.status.cancel_token,
                    2,
                ),
                daemon=True,
            ),
        ]

        return worker_threads


def create_queues(queue_items: list[CollectionFileQueueItem]) -> tuple[list[tuple[int, CollectionFileQueueItem]], queue.Queue]:
    download_queue_items: list[tuple[int, CollectionFileQueueItem]] = []
    import_queue: queue.Queue = queue.Queue()

    for queue_item in queue_items:
        download_queue_items.append(queue_item)
        import_queue.put(queue_item)

    return download_queue_items, import_queue


def get_gdrive_file_list(
    gdrive_client: GoogleDriveClient,
    logger: logging.Logger,
    modified_after: Optional[datetime] = None,
    modified_before: Optional[datetime] = None,
) -> list[GoogleDriveFile]:
    log.get_gdrive_file_list_params(logger, modified_after, modified_before)

    if modified_after or modified_before:
        gdrive_files = list(gdrive_client.list_files_by_modified_date(modified_after=modified_after, modified_before=modified_before))
    else:
        gdrive_files = list(gdrive_client.list_all_files())

    return gdrive_files
