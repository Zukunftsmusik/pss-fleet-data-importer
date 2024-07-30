import asyncio
import json
import logging
import queue
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from multiprocessing.pool import ThreadPool
from pathlib import Path
from threading import Lock
from typing import Any, Callable, Iterable, Optional, Union

import pydrive2.files
from pss_fleet_data import Collection, PssFleetDataClient
from pss_fleet_data.core.exceptions import NonUniqueTimestampError

from ..database import AsyncAutoRollbackSession, Database, crud
from ..models import CollectionFileDB, CollectionFileQueueItem
from ..models.converters import FromCollectionFileDB, FromGdriveFile
from . import CONFIG, utils
from .config import Config
from .gdrive import GoogleDriveClient, GoogleDriveFile


if CONFIG.debug_mode:
    from time import perf_counter


class Importer:
    def __init__(
        self,
        config: Config,
        database: Database,
        *,
        gdrive_folder_id: str = None,
        api_server_url: str = None,
        api_key: str = None,
        download_thread_pool_size: int = 3,
        temp_download_folder: Path = None,
    ):
        self.__config: Config = config
        self.__database: Database = database
        self.__gdrive_folder_id: str = gdrive_folder_id or self.__config.gdrive_folder_id
        self.__api_server_url: str = api_server_url or self.__config.api_default_server_url
        self.__api_key: str = api_key or self.__config.api_key
        self.__thread_pool_size: int = download_thread_pool_size
        self.__temp_download_folder: Path = temp_download_folder

        self.__logger: logging.Logger = self.__config.logger.getChild(Importer.__name__)
        self.__download_queue_items: list[tuple[int, CollectionFileQueueItem]] = []
        self.__import_queue: queue.Queue = queue.Queue()
        self.__download_thread_pool: ThreadPool = ThreadPool(download_thread_pool_size)

        self.__bulk_download_running: bool = False
        self.__bulk_download_running_lock = Lock()

        self.__bulk_import_running: bool = False
        self.__bulk_import_running_lock = Lock()

        self.__database.initialize_database(
            self.__config.db_sync_connection_str,
            self.__config.db_async_connection_str,
            self.__config.debug_mode,
            self.__config.reinitialize_database_on_startup,
        )

        self.__gdrive_client: GoogleDriveClient = GoogleDriveClient(
            config.gdrive_project_id,
            config.gdrive_private_key_id,
            config.gdrive_private_key,
            config.gdrive_client_email,
            config.gdrive_client_id,
            config.gdrive_scopes,
            self.gdrive_folder_id,
            config.gdrive_service_account_file_path,
            config.gdrive_settings_file_path,
        )

        self.__fleet_data_client: PssFleetDataClient = PssFleetDataClient(self.api_server_url, self.__api_key)

    @property
    def api_server_url(self) -> str:
        return self.__api_server_url

    @property
    def gdrive_folder_id(self) -> str:
        return self.__gdrive_folder_id

    @property
    def logger(self) -> logging.Logger:
        return self.__logger

    @property
    def temp_download_folder(self) -> Path:
        return self.__temp_download_folder or CONFIG.temp_download_folder

    @property
    def bulk_download_running(self) -> bool:
        with self.__bulk_download_running_lock:
            return self.__bulk_download_running

    @bulk_download_running.setter
    def bulk_download_running(self, running: bool):
        with self.__bulk_download_running_lock:
            self.__bulk_download_running = running

    @property
    def bulk_import_running(self) -> bool:
        with self.__bulk_import_running_lock:
            return self.__bulk_import_running

    @bulk_import_running.setter
    def bulk_import_running(self, running: bool):
        with self.__bulk_import_running_lock:
            self.__bulk_import_running = running

    def get_gdrive_file_list(self, modified_after: Optional[datetime] = None, modified_before: Optional[datetime] = None) -> list[GoogleDriveFile]:
        if self.__config.debug_mode:
            start = perf_counter()

        if modified_after or modified_before:
            if modified_after:
                if modified_before:
                    self.logger.info(
                        "Retrieving gdrive files modified after: %s and modified before: %s", modified_after.isoformat(), modified_before.isoformat()
                    )
                else:
                    self.logger.info("Retrieving gdrive files modified after: %s", modified_after.isoformat())
            else:
                self.logger.info("Retrieving gdrive files modified before: %s", modified_before.isoformat())

            gdrive_files = list(self.__gdrive_client.list_files_by_modified_date(modified_after=modified_after, modified_before=modified_before))
        else:
            self.logger.info("Retrieving all gdrive files.")
            gdrive_files = list(self.__gdrive_client.list_all_files())

        if self.__config.debug_mode:
            end = perf_counter()
            self.logger.debug(
                "Downloading list of %i file%s took %.2f seconds.",
                len(gdrive_files),
                "" if len(gdrive_files) == 1 else "s",
                end - start,
            )

        return gdrive_files

    def start_bulk_imports(self, import_queue: queue.Queue) -> asyncio.Task[None]:
        self.bulk_import_running = True
        self.logger.info("Starting import worker...")

        task = asyncio.create_task(
            self.worker_import(
                import_queue,
                self.__api_key,
                self.__fleet_data_client,
                self.logger,
            )
        )
        return task

    async def run_bulk_import(self, modified_after: Optional[datetime] = None, modified_before: Optional[datetime] = None) -> bool:
        """Runs a bulk import of files from Google Drive.

        Args:
            modified_after (datetime, optional): Specifies the time and date after which files have to be modified (created) to be considered for import. Defaults to None.
            modified_before (datetime, optional): Specifies the time and date before which files have to be modified (created) to be considered for import. Defaults to None.

        Returns:
            bool: `True`, if files have been imported. `False` if not.
        """
        log_bulk_import_start(self.logger, modified_after, modified_before)

        gdrive_files = self.get_gdrive_file_list(modified_after, modified_before)

        if not gdrive_files:
            self.logger.info("No new files found to be imported.")
            return False

        self.logger.info(f"Found {len(gdrive_files)} new gdrive files to be imported.")

        self.logger.debug("Creating database entries.")
        collection_files = [FromGdriveFile.to_collection_file(gdrive_file) for gdrive_file in gdrive_files]

        async with AsyncAutoRollbackSession(self.__database) as session:
            collection_files = await crud.insert_new_collection_files(session, collection_files)

        queue_items = FromCollectionFileDB.to_queue_items(gdrive_files, collection_files, self.temp_download_folder, self.__database)
        queue_items.sort(key=lambda queue_item: queue_item.gdrive_file_name.replace("-", "_"))

        self.logger.debug("Creating queues.")
        download_queue_items, import_queue = create_queues(queue_items)
        self.temp_download_folder.mkdir(parents=True, exist_ok=True)

        self.logger.debug("Starting workers.")

        download_count = len([_ for _ in queue_items if _.collection_file.downloaded_at is None])
        import_count = len([_ for _ in queue_items if _.collection_file.imported_at is None])
        self.logger.info(f"Downloading {download_count} Collection files and importing {import_count} Collection files.")

        download_thread = threading.Thread(target=self.worker_download, args=[download_queue_items, self.__thread_pool_size])
        download_thread.start()

        bulk_imports_task = self.start_bulk_imports(import_queue)

        self.logger.debug("Waiting for workers to finish.")

        await bulk_imports_task

        log_bulk_import_finish(self.logger, len(queue_items), modified_after, modified_before)

        return True

    async def run_import_loop(self, modified_after: Optional[datetime] = None, modified_before: Optional[datetime] = None):
        while True:
            if not modified_after:
                async with AsyncAutoRollbackSession(self.__database) as session:
                    last_imported_file = await crud.get_latest_imported_collection_file(session)
                    modified_after = utils.get_next_full_hour(last_imported_file.timestamp) if last_imported_file else None

            did_import = await self.run_bulk_import(modified_after=modified_after, modified_before=modified_before)

            if did_import:
                continue

            now = utils.get_now()
            wait_until = utils.get_next_full_hour(now) + timedelta(minutes=1)
            wait_for_seconds = (wait_until - now).total_seconds()
            self.logger.info("Waiting for %.2f seconds until next import run at %s.", wait_for_seconds, wait_until.isoformat())
            await asyncio.sleep(wait_for_seconds)

    def worker_download(self, download_queue_items: list[CollectionFileQueueItem], thread_pool_size):
        self.bulk_download_running = True
        executor = utils.run_async_thread_pool_executor(self.download_gdrive_file, download_queue_items, thread_pool_size)
        executor.shutdown()
        self.bulk_download_running = False

    async def worker_import(
        self,
        import_queue: queue.Queue,
        api_key: str,
        fleet_data_client: PssFleetDataClient,
        parent_logger: logging.Logger,
    ):
        logger = parent_logger.parent.getChild("importWorker")
        file_no: int
        queue_item: CollectionFileQueueItem

        while True:
            try:
                file_no, queue_item = import_queue.get(block=False)
            except queue.Empty:
                if not self.bulk_download_running:
                    break
                await asyncio.sleep(1)
                continue

            await wait_until_file_downloaded(logger, file_no, queue_item)
            if queue_item.error_while_downloading:
                logger.info("Error while downloading, skipping file no %i: %s", file_no, queue_item.gdrive_file_name)
                continue

            if await check_if_file_empty(logger, file_no, queue_item):
                logger.info("Skipping empty file no %i: %s", file_no, queue_item.download_file_path)
                continue

            logger.debug("Importing file %i: %s", file_no, queue_item.download_file_path)

            try:
                collection_metadata = await fleet_data_client.upload_collection(str(queue_item.download_file_path), api_key=api_key)
                logger.info("Imported file %i (Collection ID: %i): %s", file_no, collection_metadata.collection_id, queue_item.download_file_path)
            except NonUniqueTimestampError:
                collection_metadata = await fleet_data_client.get_most_recent_collection_metadata_by_timestamp(queue_item.collection_file.timestamp)
                logger.info(
                    "Skipped file %i (Collection already exists with ID: %i): %s",
                    file_no,
                    collection_metadata.collection_id,
                    queue_item.download_file_path,
                )

            if collection_metadata:
                await queue_item.update_collection_file(imported_at=utils.remove_timezone(datetime.now(tz=timezone.utc)))

                if not self.__config.keep_downloaded_files:
                    queue_item.download_file_path.unlink(missing_ok=True)

            import_queue.task_done()

        self.bulk_import_running = False
        parent_logger.info("Import worker finished.")

    async def download_gdrive_file(self, file_no: int, queue_item: CollectionFileQueueItem) -> CollectionFileQueueItem:
        logger = self.logger.getChild("downloadWorker")
        logger.debug("Downloading file no. %i: %s", file_no, queue_item.gdrive_file_name)

        downloaded_file_path = None
        target_file_path = queue_item.target_directory.joinpath(queue_item.gdrive_file_name)
        exists = False
        if target_file_path.exists():
            if target_file_path.stat().st_size > 8192:
                downloaded_file_path = target_file_path
                logger.debug("File no. %i already exists: %s", file_no, target_file_path)
                exists = True
            else:
                target_file_path.unlink(missing_ok=True)
                exists = False

        if exists is False:
            try:
                downloaded_file_path = self.__gdrive_client.download_file(queue_item.gdrive_file, queue_item.target_directory)
                logger.debug("File no. %i downloaded: %s", file_no, target_file_path)
            except pydrive2.files.ApiRequestError as exc:
                if self.__config.debug_mode:
                    logger.error("An error occured while downloading the file '%s' from Drive.", queue_item.gdrive_file_name, exc_info=exc)
                else:
                    logger.error("An error occured while downloading the file '%s' from Drive.", queue_item.gdrive_file_name)
                queue_item.error_while_downloading = True
                downloaded_file_path = None

        if downloaded_file_path:
            queue_item.download_file_path = downloaded_file_path
            await queue_item.update_collection_file(downloaded_at=utils.remove_timezone(datetime.now(tz=timezone.utc)))
            logger.debug("Updated queue item for file no. %i.", file_no)


def log_bulk_import_start(logger: logging.Logger, modified_after: Optional[datetime], modified_before: Optional[datetime]):
    if modified_after:
        if modified_before:
            logger.info(
                "Starting bulk import of files modified after %s & modified before %s.", modified_after.isoformat(), modified_before.isoformat()
            )
        else:
            logger.info("Starting bulk import of files modified after %s.", modified_after.isoformat())
    else:
        if modified_before:
            logger.info("Starting bulk import of files modified before %s.", modified_before.isoformat())
        else:
            logger.info("Starting bulk import.")


def log_bulk_import_finish(logger: logging.Logger, file_count: int, modified_after: Optional[datetime], modified_before: Optional[datetime]):
    if modified_after:
        if modified_before:
            logger.info(
                "Finished bulk import of %i files modified after %s & modified before %s.",
                file_count,
                modified_after.isoformat(),
                modified_before.isoformat(),
            )
        else:
            logger.info("Finished bulk import of %i files modified after %s.", file_count, modified_after.isoformat())
    else:
        if modified_before:
            logger.info("Finished bulk import of %i files modified before %s.", file_count, modified_before.isoformat())
        else:
            logger.info("Finished bulk import of %i files.", file_count)


def create_queues(queue_items: list[CollectionFileQueueItem]) -> tuple[list[tuple[int, CollectionFileQueueItem]], queue.Queue]:
    download_queue_items: list[tuple[int, CollectionFileQueueItem]] = []
    import_queue: queue.Queue = queue.Queue()

    for i, queue_item in enumerate(queue_items):
        download_queue_items.append((i + 1, queue_item))
        import_queue.put((i + 1, queue_item))

    return download_queue_items, import_queue


async def wait_until_file_downloaded(logger: logging.Logger, file_no: int, queue_item: CollectionFileQueueItem):
    waiting = False
    while queue_item.download_file_path is None and not queue_item.error_while_downloading:
        if not waiting:
            logger.debug("Waiting with import until file %i is downloaded: %s", file_no, queue_item.gdrive_file_name)
            waiting = True
        # Wait until file is downloaded, since multiple downloads are running simultaneously, but import needs to be done in order.
        await asyncio.sleep(0.2)


async def check_if_file_empty(logger: logging.Logger, file_no: int, queue_item: CollectionFileQueueItem, read_tries: Optional[int] = None) -> bool:
    if read_tries is None:
        read_tries = 5

    file_is_empty = False
    not_content = False

    waiting = False
    for _ in range(5):
        with open(queue_item.download_file_path) as fp:
            try:
                content = json.load(fp)
            except json.decoder.JSONDecodeError as exc:
                if "Expecting value" in exc.msg:
                    if not waiting:
                        logger.debug("File is empty. Waiting with import until file %i is fully downloaded: %s", file_no, queue_item.gdrive_file_name)
                        waiting = True
                    file_is_empty = True
                    await asyncio.sleep(0.2)
                    continue
                raise

            if not content:
                not_content = True
                break

    if not_content:
        return True

    if file_is_empty and read_tries == 0:
        return True
