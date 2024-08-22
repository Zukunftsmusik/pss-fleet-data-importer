import asyncio
import threading
from datetime import datetime, timedelta
from typing import Iterable, Optional

from httpx import ConnectError
from pss_fleet_data import PssFleetDataClient

from ..converters import FromCollectionFileDB, FromGdriveFile
from ..core import utils
from ..core.config import Config
from ..core.gdrive import GDriveFile, GoogleDriveClient
from ..core.models.cancellation_token import CancellationToken
from ..core.models.collection_file_change import CollectionFileChange
from ..core.models.filesystem import FileSystem
from ..database import DatabaseRepository, crud
from ..database.models import CollectionFileDB
from ..database.unit_of_work import AbstractUnitOfWork, SqlModelUnitOfWork
from ..log.log_importer import importer as log
from ..models import ImportStatus, QueueItem
from . import download_worker, import_worker


class Importer:
    def __init__(
        self,
        config: Config,
        gdrive_client: GoogleDriveClient,
        pss_fleet_data_client: PssFleetDataClient,
        filesystem: FileSystem = FileSystem(),
    ):
        self.config: Config = config
        self.gdrive_client: GoogleDriveClient = gdrive_client
        self.fleet_data_client: PssFleetDataClient = pss_fleet_data_client
        self.filesystem = filesystem

        self.status = ImportStatus()

    def cancel_workers(self):
        log.workers_cancel()
        self.status.cancel_token.cancel()

    async def check_api_server_connection(self) -> bool:
        try:
            await self.fleet_data_client.ping()
            return True
        except ConnectError:
            return False

    async def run_import_loop(
        self,
        run_once: bool = False,
        modified_after: Optional[datetime] = None,
        modified_before: Optional[datetime] = None,
        filesystem: FileSystem = FileSystem(),
    ):
        cancel_message = "Import cancelled. Exiting import loop."

        import_modified_after = await get_updated_modified_after(modified_after=modified_after)

        while True:
            if self.status.cancel_token.log_if_cancelled(cancel_message):
                break

            if import_modified_after and modified_before and import_modified_after >= modified_before:
                break

            if import_modified_after and utils.get_next_full_hour(import_modified_after) > utils.get_now():
                await wait_for_next_import()
            else:
                import_modified_after = await self.run_bulk_import(
                    modified_after=import_modified_after, modified_before=modified_before, filesystem=filesystem
                )
                import_modified_after = utils.get_next_full_hour(import_modified_after)

                if run_once:
                    break

    async def run_bulk_import(
        self,
        modified_after: Optional[datetime] = None,
        modified_before: Optional[datetime] = None,
        filesystem: FileSystem = FileSystem(),
    ) -> datetime:
        start = utils.get_now()
        log.bulk_import_start_time(start)
        log.bulk_import_start(modified_after, modified_before)

        log.download_gdrive_file_list_params(modified_after=modified_after, modified_before=modified_before)
        gdrive_files = get_gdrive_file_list(self.gdrive_client, modified_after=modified_after, modified_before=modified_before)

        log.download_gdrive_file_list_length(len(gdrive_files))
        if not gdrive_files:
            return modified_after

        collection_files = create_collection_files(gdrive_files)
        collection_files = await insert_new_collection_files(collection_files)

        log.queue_items_create()
        queue_items = FromCollectionFileDB.to_queue_items(gdrive_files, collection_files, self.config.temp_download_folder, self.status.cancel_token)

        log.download_folder_create(self.config.temp_download_folder)
        filesystem.mkdir(self.config.temp_download_folder, create_parents=True, exist_ok=True)

        log.downloads_imports_count(queue_items)

        download_worker_thread = create_download_worker_thread(
            queue_items,
            self.gdrive_client,
            self.config.download_thread_pool_size,
            self.config.debug_mode,
            self.status.cancel_token,
            filesystem=filesystem,
        )
        download_worker_thread.start()

        for queue_item in queue_items:
            await wait_for_item_download(queue_item)

            if queue_item.status.download_timed_out:
                break

            if queue_item.status.download_error:
                await update_database(CollectionFileChange(collection_file_id=queue_item.collection_file_id, error=True), queue_item.item_no)
                continue

            await import_worker.process_queue_item(queue_item, self.fleet_data_client, self.config.keep_downloaded_files, filesystem=filesystem)

            if queue_item.status.import_error:
                await update_database(
                    CollectionFileChange(collection_file_id=queue_item.collection_file_id, imported=False, error=True),
                    queue_item.item_no,
                )
            else:
                await update_database(
                    CollectionFileChange(collection_file_id=queue_item.collection_file_id, imported=True, error=False),
                    queue_item.item_no,
                )

        download_worker_thread.join()

        end = utils.get_now()
        log.bulk_import_finish(queue_items, modified_after, modified_before)
        log.bulk_import_finish_time(len(queue_items), start, end)

        last_imported_file_modified_date = max((queue_item.gdrive_file.modified_date for queue_item in queue_items if queue_item.status.done))
        return last_imported_file_modified_date


async def get_updated_modified_after(modified_after: Optional[datetime] = None):
    async with DatabaseRepository.get_session() as session:
        latest_imported_modified_date = await crud.get_latest_imported_gdrive_modified_date(session)

    if latest_imported_modified_date:
        latest_imported_modified_date = utils.get_next_full_hour(latest_imported_modified_date)

        if modified_after:
            updated_modified_after = max(modified_after, latest_imported_modified_date)
        else:
            updated_modified_after = latest_imported_modified_date

        return utils.get_next_full_hour(updated_modified_after)

    return modified_after


def create_download_worker_thread(
    queue_items: Iterable[QueueItem],
    gdrive_client: GoogleDriveClient,
    thread_pool_size: int,
    debug_mode: bool,
    cancel_token: CancellationToken,
    filesystem: FileSystem = FileSystem(),
) -> threading.Thread:
    download_worker_thread = threading.Thread(
        target=download_worker.worker,
        name="Download worker",
        args=[
            queue_items,
            gdrive_client,
            thread_pool_size,
            debug_mode,
            cancel_token,
        ],
        kwargs={
            "worker_timeout": 60.0,
            "filesystem": filesystem,
        },
        daemon=True,
    )

    return download_worker_thread


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

    gdrive_files = sorted(gdrive_files, key=lambda gdrive_file: gdrive_file.name.replace("-", "_"))
    return gdrive_files


async def insert_new_collection_files(collection_files: Iterable[CollectionFileDB], uow: Optional[AbstractUnitOfWork] = None):
    uow = uow or SqlModelUnitOfWork()

    log.database_entries_create()
    async with uow:
        existing_collection_files = await uow.collection_files.list_files(
            gdrive_file_ids=[collection_file.gdrive_file_id for collection_file in collection_files]
        )
        existing_gdrive_file_ids = [collection_file.gdrive_file_id for collection_file in existing_collection_files]

        new_collection_files = [
            collection_file for collection_file in collection_files if collection_file.gdrive_file_id not in existing_gdrive_file_ids
        ]

        for collection_file in new_collection_files:
            uow.collection_files.add(collection_file)

        await uow.commit()

        new_collection_files = await uow.collection_files.refresh_files(new_collection_files)

        result = list(existing_collection_files) + new_collection_files
        return result


async def wait_for_item_download(queue_item: QueueItem):
    while not queue_item.status.downloaded and not queue_item.status.download_error:
        await asyncio.sleep(0.1)


async def wait_for_next_import():
    now = utils.get_now()
    wait_until = utils.get_next_full_hour(now) + timedelta(minutes=1)
    wait_for_seconds = (wait_until - utils.get_now()).total_seconds()

    log.wait_for_import(wait_for_seconds, wait_until)
    await asyncio.sleep(wait_for_seconds)


async def update_database(change: CollectionFileChange, item_no: int, uow: Optional[AbstractUnitOfWork] = None):
    uow = uow or SqlModelUnitOfWork()

    async with uow:
        collection_file = await uow.collection_files.get_by_id(change.collection_file_id)

        if collection_file:
            if change.imported is not None:
                collection_file.imported = change.imported
            if change.error is not None:
                collection_file.error = change.error

            collection_file = uow.collection_files.add(collection_file)
            await uow.commit()

            log.queue_item_update(item_no, change)
