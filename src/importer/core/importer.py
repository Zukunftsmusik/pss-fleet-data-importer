import asyncio
import json
import logging
import queue
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pydrive2.files
from cancel_token import CancellationToken
from pss_fleet_data import PssFleetDataClient
from pss_fleet_data.core.exceptions import NonUniqueTimestampError
from pydrive2.files import ApiRequestError

from ..database import AsyncAutoRollbackSession, Database, crud
from ..models import CollectionFileChange, CollectionFileQueueItem, ImportStatus
from ..models.converters import FromCollectionFileDB, FromGdriveFile
from . import utils, wrapper
from .config import Config, get_config
from .gdrive import GoogleDriveClient, GoogleDriveFile


if get_config().debug_mode:
    pass


class Importer:
    def __init__(
        self,
        config: Config,
        database: Database,
        gdrive_folder_id: str = None,
        api_server_url: str = None,
        api_key: str = None,
        download_thread_pool_size: int = 3,
        temp_download_folder: Path = None,
    ):
        self.__config: Config = config

        self.api_server_url: str = api_server_url or self.__config.api_default_server_url
        self.gdrive_folder_id: str = gdrive_folder_id or self.__config.gdrive_folder_id
        self.logger: logging.Logger = self.__config.logger.getChild(Importer.__name__)
        self.status = ImportStatus()
        self.thread_pool_size: int = download_thread_pool_size
        self.temp_download_folder: Path = temp_download_folder or get_config().temp_download_folder

        self.__database: Database = database
        self.__api_key: str = api_key or self.__config.api_key

        self.__download_queue_items: list[CollectionFileQueueItem] = []
        self.__import_queue: queue.Queue = queue.Queue()
        self.__database_queue: queue.Queue = queue.Queue()

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

    def cancel_workers(self):
        self.logger.warn("Cancelling workers.")
        self.status.cancel_token.cancel()

    def get_gdrive_file_list(self, modified_after: Optional[datetime] = None, modified_before: Optional[datetime] = None) -> list[GoogleDriveFile]:
        log_get_gdrive_file_list_params(self.logger, modified_after, modified_before)

        if modified_after or modified_before:
            gdrive_files = list(self.__gdrive_client.list_files_by_modified_date(modified_after=modified_after, modified_before=modified_before))
        else:
            gdrive_files = list(self.__gdrive_client.list_all_files())

        return gdrive_files

    async def run_import_loop(self, modified_after: Optional[datetime] = None, modified_before: Optional[datetime] = None):
        while not self.status.cancel_token.cancelled:
            while self.status.bulk_database_running:
                await asyncio.sleep(0.1)

            after = modified_after
            if not modified_after:
                async with AsyncAutoRollbackSession(self.__database) as session:
                    last_imported_file = await crud.get_latest_imported_collection_file(session)
                    after = utils.get_next_full_hour(last_imported_file.timestamp) if last_imported_file else None

            did_import = await self.run_bulk_import(modified_after=after, modified_before=modified_before)

            if did_import:
                await asyncio.sleep(1)  # Wait a second for database transactions to finish
                continue

            now = utils.get_now()
            wait_until = utils.get_next_full_hour(now) + timedelta(minutes=1)
            wait_for_seconds = (wait_until - now).total_seconds()
            self.logger.info("Waiting for %.2f seconds until next import run at %s.", wait_for_seconds, wait_until.isoformat())
            await asyncio.sleep(wait_for_seconds)

    async def run_bulk_import(self, modified_after: Optional[datetime] = None, modified_before: Optional[datetime] = None) -> bool:
        print(f"### Starting bulk import at: {datetime.now(tz=timezone.utc).isoformat()}")

        log_bulk_import_start(self.logger, modified_after, modified_before)

        gdrive_files = wrapper.debug_log_running_time(self.logger, "Downloading file list")(
            self.get_gdrive_file_list,
            modified_after,
            modified_before,
        )

        if not gdrive_files:
            self.logger.info("No new files found to be imported.")
            return False

        self.logger.info(f"Found {len(gdrive_files)} new gdrive files to be imported.")

        self.logger.debug("Creating database entries.")
        collection_files = [FromGdriveFile.to_collection_file(gdrive_file) for gdrive_file in gdrive_files]
        async with AsyncAutoRollbackSession(self.__database) as session:
            collection_files = await crud.insert_new_collection_files(session, collection_files)

        self.logger.debug("Creating queue items.")
        queue_items = FromCollectionFileDB.to_queue_items(gdrive_files, collection_files, self.temp_download_folder, self.status.cancel_token)
        queue_items.sort(key=lambda queue_item: queue_item.gdrive_file_name.replace("-", "_"))  # There're files where some underscores are hyphens.

        self.logger.debug("Creating queues from queue items.")
        self.__download_queue_items, self.__import_queue = create_queues(queue_items)

        self.logger.debug("Ensuring that path '%s' exists.", self.temp_download_folder)
        self.temp_download_folder.mkdir(parents=True, exist_ok=True)

        log_downloads_imports(self.logger, queue_items)

        download_thread = threading.Thread(
            target=utils.start_pooled_bulk_operation,
            args=[
                self.thread_pool_size,
                self.worker_download,
                self.__download_queue_items,
            ],
            kwargs={
                "cancel_token": self.status.cancel_token,
                "status_flag": self.status.bulk_download_running,
            },
            daemon=True,
        )
        self.logger.info("Starting download worker...")
        download_thread.start()

        database_thread = utils.create_async_thread(
            self.worker_db, self.__database, self.__database_queue, self.logger, self.status.cancel_token, daemon=True
        )
        self.logger.info("Starting database worker...")
        database_thread.start()

        # import_thread = utils.create_async_thread(
        #     self.worker_import,
        #     self.__import_queue,
        #     self.__database_queue,
        #     self.__api_key,
        #     self.__fleet_data_client,
        #     self.logger,
        #     self.status.cancel_token,
        #     self.__config.keep_downloaded_files,
        #     daemon=True,
        # )
        # self.logger.info("Starting import worker...")
        # import_thread.start()

        while self.status.bulk_download_running.value or self.status.bulk_import_running.value:
            await asyncio.sleep(1)

        log_bulk_import_finish(self.logger, len(queue_items), modified_after, modified_before)
        print(f"### Finished bulk import of {len(queue_items)} files at: {datetime.now(tz=timezone.utc).isoformat()}")

        return True

    async def worker_db(self, database: Database, database_queue: queue.Queue, parent_logger: logging.Logger, cancel_token: CancellationToken):
        logger = parent_logger.parent.getChild("databaseWorker")
        queue_item: CollectionFileQueueItem
        change: CollectionFileChange

        while not cancel_token.cancelled:
            try:
                queue_item, change = database_queue.get(block=False)
            except queue.Empty:
                await asyncio.sleep(1)
                continue

            self.status.bulk_database_running = True

            await queue_item.update_collection_file(database, change)
            log_queue_item_update(logger, queue_item, change)

            database_queue.task_done()

            self.status.bulk_database_running = False

        if cancel_token.cancelled:
            parent_logger.info("Database worker cancelled.")
        else:
            parent_logger.info("Database worker finished.")

    def worker_download(self, queue_item: CollectionFileQueueItem) -> CollectionFileQueueItem:
        logger = self.logger.getChild("downloadWorker")
        if queue_item.cancel_token.cancelled:
            return

        logger.debug("Downloading file no. %i: %s", queue_item.item_no, queue_item.gdrive_file_name)

        downloaded_file_path = None
        exists = check_if_exists(queue_item)

        if queue_item.cancel_token.cancelled:
            return

        if exists:
            downloaded_file_path = queue_item.target_file_path
            logger.debug("File no. %i already exists: %s", queue_item.item_no, downloaded_file_path)
        else:
            queue_item.target_file_path.unlink(missing_ok=True)  # File also counts as not existing, if the file size differs from the file on gdrive

            for _ in range(3):
                try:
                    downloaded_file_path = self.__gdrive_client.download_file(queue_item.gdrive_file, queue_item.target_directory_path)
                    logger.debug("File no. %i downloaded: %s", queue_item.item_no, queue_item.target_file_path)
                    queue_item.error_while_downloading = False
                    break
                except pydrive2.files.ApiRequestError as exc:
                    log_gdrive_error(logger, queue_item, self.__config.debug_mode, exc)
                    queue_item.error_while_downloading = True

        if downloaded_file_path:
            if queue_item.cancel_token.cancelled:
                return

            queue_item.download_file_path = downloaded_file_path
            self.__database_queue.put((queue_item, CollectionFileChange(downloaded_at=utils.get_now())))

    async def worker_import(
        self,
        import_queue: queue.Queue,
        database_queue: queue.Queue,
        api_key: str,
        fleet_data_client: PssFleetDataClient,
        parent_logger: logging.Logger,
        cancel_token: CancellationToken,
        keep_downloaded_files: bool = False,
    ):
        self.status.bulk_import_running.value = True
        logger = parent_logger.parent.getChild("importWorker")

        while not cancel_token.cancelled:
            try:
                queue_item: CollectionFileQueueItem = import_queue.get(block=False)
            except queue.Empty:
                break

            if await skip_file_import_on_error(logger, queue_item.item_no, queue_item):
                logger.error("Could not import file %i: %s", queue_item.item_no, queue_item.gdrive_file_name)
                import_queue.task_done()
                continue

            logger.debug("Importing file %i: %s", queue_item.item_no, queue_item.download_file_path)

            try:
                collection_metadata = await fleet_data_client.upload_collection(str(queue_item.download_file_path), api_key=api_key)
                imported_at = utils.remove_timezone(datetime.now(tz=timezone.utc))
                logger.info(
                    "Imported file %i (Collection ID: %i): %s", queue_item.item_no, collection_metadata.collection_id, queue_item.download_file_path
                )
            except NonUniqueTimestampError:
                imported_at = utils.remove_timezone(datetime.now(tz=timezone.utc))
                collection_metadata = await fleet_data_client.get_most_recent_collection_metadata_by_timestamp(queue_item.collection_file.timestamp)
                logger.info(
                    "Skipped file %i (Collection already exists with ID: %i): %s",
                    queue_item.item_no,
                    collection_metadata.collection_id,
                    queue_item.download_file_path,
                )
            except Exception as exc:
                logger.error("Could not import file %i: %s", queue_item.item_no, queue_item.gdrive_file_name)
                logger.error(exc, exc_info=True)
                import_queue.task_done()
                continue

            if collection_metadata:
                database_queue.put((queue_item, CollectionFileChange(imported_at=imported_at)))

                if not keep_downloaded_files:
                    queue_item.download_file_path.unlink(missing_ok=True)

            import_queue.task_done()

        if cancel_token.cancelled:
            parent_logger.info("Import worker cancelled.")
        else:
            parent_logger.info("Import worker finished.")

        self.status.bulk_import_running.value = False


def check_if_exists(queue_item: CollectionFileQueueItem):
    if queue_item.target_file_path.exists():
        file_size = queue_item.target_file_path.stat().st_size
        if file_size == queue_item.gdrive_file_size:
            return True
    return False


async def check_if_file_empty(logger: logging.Logger, file_no: int, queue_item: CollectionFileQueueItem, read_tries: Optional[int] = 5) -> bool:
    if read_tries is None:
        read_tries = 5

    file_is_empty = False
    waiting = False

    for _ in range(read_tries):
        with open(queue_item.download_file_path) as fp:
            try:
                json_content = json.load(fp)
                file_is_empty = False
            except json.decoder.JSONDecodeError as exc:
                if "Expecting value" in exc.msg:
                    if not waiting:
                        logger.debug("File is empty. Waiting with import until file %i is fully downloaded: %s", file_no, queue_item.gdrive_file_name)
                        waiting = True
                    file_is_empty = True
                    await asyncio.sleep(0.2)
                    continue
                raise

            if not json_content:
                return True

    if file_is_empty:
        return True

    return False


def create_queues(queue_items: list[CollectionFileQueueItem]) -> tuple[list[tuple[int, CollectionFileQueueItem]], queue.Queue]:
    download_queue_items: list[tuple[int, CollectionFileQueueItem]] = []
    import_queue: queue.Queue = queue.Queue()

    for queue_item in queue_items:
        download_queue_items.append(queue_item)
        import_queue.put(queue_item)

    return download_queue_items, import_queue


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


def log_downloads_imports(logger: logging.Logger, queue_items: list[CollectionFileQueueItem]):
    download_count = len([_ for _ in queue_items if _.collection_file.downloaded_at is None])
    import_count = len([_ for _ in queue_items if _.collection_file.imported_at is None])
    logger.info(f"Downloading {download_count} Collection files and importing {import_count} Collection files.")


def log_get_gdrive_file_list_params(logger: logging.Logger, modified_after: Optional[datetime], modified_before: Optional[datetime]):
    if modified_after or modified_before:
        if modified_after:
            if modified_before:
                logger.info(
                    "Retrieving gdrive files modified after: %s and modified before: %s", modified_after.isoformat(), modified_before.isoformat()
                )
            else:
                logger.info("Retrieving gdrive files modified after: %s", modified_after.isoformat())
        else:
            logger.info("Retrieving gdrive files modified before: %s", modified_before.isoformat())
    else:
        logger.info("Retrieving all gdrive files.")


def log_gdrive_error(logger: logging.Logger, queue_item: CollectionFileQueueItem, log_exception: bool, exc: ApiRequestError):
    msg = f"An error occured while downloading the file no. {queue_item.item_no} '{queue_item.gdrive_file_name}' from Drive."
    if log_exception:
        logger.error(msg, exc_info=exc)
    else:
        logger.error("%s:  %s", msg, type(exc))


async def wait_until_file_downloaded(logger: logging.Logger, file_no: int, queue_item: CollectionFileQueueItem):
    waiting = False
    while (queue_item.collection_file.downloaded_at is None or queue_item.download_file_path is None) and not queue_item.error_while_downloading:
        if not waiting:
            logger.debug("Waiting with import until file %i is downloaded: %s", file_no, queue_item.gdrive_file_name)
            waiting = True
        await asyncio.sleep(0.1)


async def skip_file_import_on_error(logger: logging.Logger, file_no: int, queue_item: CollectionFileQueueItem) -> bool:
    if queue_item.cancel_token.cancelled:
        return True

    # Wait until file is downloaded, since multiple downloads are running simultaneously, but import needs to be done in order.
    await wait_until_file_downloaded(logger, file_no, queue_item)
    if queue_item.error_while_downloading:
        logger.warn("Error while downloading. Skipping file no. %i: %s", file_no, queue_item.gdrive_file_name)
        return True

    # Sometimes the file is flagged as downloaded, but still empty on disk
    if await check_if_file_empty(logger, file_no, queue_item):
        logger.warn("File is empty. Skipping file no. %i: %s", file_no, queue_item.download_file_path)
        return True

    with open(queue_item.download_file_path, "r") as fp:
        contents = json.load(fp)
        if not contents:
            logger.warn("File contains empty json. Skipping file no. %i: %s", file_no, queue_item.download_file_path)
            return True

    return False


def log_queue_item_update(logger: logging.Logger, queue_item: CollectionFileQueueItem, change: CollectionFileChange):
    logger.debug("Updated queue item no. %i: %s", queue_item.item_no, change)
