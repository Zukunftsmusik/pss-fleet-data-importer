import asyncio
import logging
import queue
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pss_fleet_data import PssFleetDataClient

from ..database import crud
from ..database.db import AsyncAutoRollbackSession, Database
from ..models import CollectionFileDB
from . import CONFIG, utils
from .config import Config
from .gdrive import GoogleDriveClient, GoogleDriveFile


if CONFIG.debug_mode:
    from time import perf_counter


class Importer:
    def __init__(self, config: Config, database: Database, *, gdrive_folder_id: str = None, api_server_url: str = None, api_key: str = None):
        self.__config: Config = config
        self.__database: Database = database
        self.__gdrive_folder_id: str = gdrive_folder_id or self.__config.gdrive_folder_id
        self.__api_server_url: str = api_server_url or self.__config.api_default_server_url
        self.__api_key: str = api_key or self.__config.api_key

        self.__logger: logging.Logger = self.__config.logger.getChild(Importer.__name__)
        self.__download_bulk_queue: queue.Queue = queue.Queue()
        self.__import_bulk_queue: queue.Queue = queue.Queue()

        self.__bulk_download_running: bool = False
        self.__bulk_download_running_lock = threading.Lock()

        self.__bulk_import_running: bool = False
        self.__bulk_import_running_lock = threading.Lock()

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

    def get_bulk_download_running(self) -> bool:
        with self.__bulk_download_running_lock:
            return self.__bulk_download_running

    def get_bulk_import_running(self) -> bool:
        with self.__bulk_import_running_lock:
            return self.__bulk_import_running

    def get_gdrive_file_list(self, modified_after: Optional[datetime] = None, modified_before: Optional[datetime] = None) -> list[GoogleDriveFile]:
        before = datetime.now(tz=timezone.utc)
        self.logger.debug("Start getting gdrive file list at: %s", before.isoformat())
        if modified_after or modified_before:
            if modified_after:
                if modified_before:
                    self.logger.debug(
                        "Retrieving gdrive files modified after: %s and modified before: %s", modified_after.isoformat(), modified_before.isoformat()
                    )
                else:
                    self.logger.debug("Retrieving gdrive files modified after: %s", modified_after.isoformat())
            else:
                self.logger.debug("Retrieving gdrive files modified before: %s", modified_before.isoformat())
            gdrive_files = list(self.__gdrive_client.list_files_by_modified_date(modified_after=modified_after, modified_before=modified_before))
        else:
            self.logger.debug("Retrieving all gdrive files.")
            gdrive_files = list(self.__gdrive_client.list_all_files())
        after = datetime.now(tz=timezone.utc)
        self.logger.debug("Finish getting gdrive file list at: %s", after.isoformat())
        self.logger.debug(
            "Downloading list of %i file%s took %.2f seconds.",
            len(gdrive_files),
            "" if len(gdrive_files) == 1 else "s",
            (after - before).total_seconds(),
        )

        return gdrive_files

    def set_bulk_downloads_running(self, running: bool):
        with self.__bulk_download_running_lock:
            self.__bulk_download_running = running

    def set_bulk_imports_running(self, running: bool):
        with self.__bulk_import_running_lock:
            self.__bulk_import_running = running

    def start_bulk_downloads(self) -> asyncio.Task[None]:
        self.set_bulk_downloads_running(True)
        self.logger.info("Starting download worker...")

        task = asyncio.create_task(
            self.worker_download(
                self.__download_bulk_queue,
                self.__import_bulk_queue,
                self.__database,
                self.__gdrive_client,
                self.__config.temp_download_folder,
            )
        )
        return task

    def start_bulk_imports(self, import_queue: queue.Queue) -> asyncio.Task[None]:
        self.set_bulk_imports_running(True)
        self.logger.info("Starting import worker...")

        task = asyncio.create_task(self.worker_import(import_queue))
        return task

    async def run_bulk_import(self, modified_after: Optional[datetime] = None, modified_before: Optional[datetime] = None):
        gdrive_files = self.get_gdrive_file_list(modified_after, modified_before)

        # Fill Queue
        self.logger.debug("Filling download queue.")
        for gdrive_file in gdrive_files:
            self.__download_bulk_queue.put(gdrive_file)

        bulk_downloads_task = self.start_bulk_downloads()
        # bulk_imports_handler = self.start_bulk_imports(self.__import_bulk_queue)

        await bulk_downloads_task
        # bulk_imports_handler.join()

    async def run_import_loop(self):
        # Get latest file in DB
        while True:
            return
            # Wait until full hour (or 1 minute after that)
            # List files on gdrive modified after lastest file in DB
            # Download those files and import them (without queue)
            # Repeat

    async def worker_download(
        self,
        download_queue: queue.Queue,
        import_queue: queue.Queue,
        database: Database,
        gdrive_client: GoogleDriveClient,
        target_dir: Path,
    ):
        target_dir.mkdir(parents=True, exist_ok=True)

        if self.__config.debug_mode:
            perf_all_start = perf_counter()
            file_count = 0

        while True:
            if self.__config.debug_mode:
                perf_start = perf_counter()

            try:
                gdrive_file: GoogleDriveFile = download_queue.get(block=False)
                if self.__config.debug_mode:
                    file_count += 1
            except queue.Empty:
                break

            file_name: str = gdrive_file["title"]
            gdrive_file_id: str = gdrive_file["id"]
            timestamp = datetime.strptime(file_name, "pss-top-100_%Y%m%d-%H%M%S.json")
            self.logger.debug("Downloading file '%s' with ID '%s'.", file_name, gdrive_file_id)

            async with AsyncAutoRollbackSession(database.async_engine) as session:
                collection_file = await self.get_collection_file_db(database, gdrive_file_id, file_name, timestamp)

            target_file_path = target_dir.joinpath(file_name)
            if target_file_path.exists():
                downloaded_file_path = target_file_path
            else:
                downloaded_file_path = gdrive_client.download_file(gdrive_file, target_dir)

            if downloaded_file_path:
                collection_file.downloaded_at = utils.remove_timezone(datetime.now(tz=timezone.utc))
                async for session in database.get_session():
                    collection_file = await crud.save_collection_file(session, collection_file)

                import_queue.put((downloaded_file_path, collection_file))

            download_queue.task_done()

            self.logger.debug("Downloaded file '%s' with ID '%s' to: %s", file_name, gdrive_file_id, downloaded_file_path)
            if self.__config.debug_mode:
                self.logger.debug("Time elapsed: %.4f seconds", perf_counter() - perf_start)

        download_queue.join()
        self.set_bulk_downloads_running(False)
        self.logger.info("Download worker finished.")

        if self.__config.debug_mode:
            total_seconds = perf_counter() - perf_all_start
            self.logger.debug(
                "Downloading %i files took %.4f seconds (%.4f seconds per file).", file_count, total_seconds, total_seconds / file_count
            )

    async def get_collection_file_db(self, database: Database, gdrive_file_id: str, file_name: str, timestamp: datetime) -> CollectionFileDB:
        async with AsyncAutoRollbackSession(database.async_engine) as session:
            collection_file = await crud.get_collection_file_by_gdrive_file_id(session, gdrive_file_id)
            if collection_file:
                return collection_file

            collection_file = CollectionFileDB(
                gdrive_file_id=gdrive_file_id,
                file_name=file_name,
                timestamp=timestamp,
            )
            collection_file = await crud.save_collection_file(session, collection_file)
            return collection_file

    async def worker_import(self, import_queue: queue.Queue, api_key: str, fleet_data_client: PssFleetDataClient):
        while True:
            file_path: Path
            collection_file: CollectionFileDB
            try:
                file_path, collection_file = import_queue.get(block=False)
            except queue.Empty:
                if not self.get_bulk_download_running():
                    break
                asyncio.sleep(1)

            self.logger.debug("Importing file '%s' to Fleet Data API.", file_path)
            collection = await fleet_data_client.upload_collection(file_path, api_key=api_key)

            if collection.collection_id:
                collection_file.imported_at = datetime.now(tz=timezone.utc)
                async with AsyncAutoRollbackSession as session:
                    collection_file = await crud.save_collection_file(session, collection_file)

                file_path.unlink(missing_ok=True)

            import_queue.task_done()
            raise NotImplementedError()

        self.set_bulk_imports_running(False)
        self.logger.info("Import worker finished.")
        self.logger.info("Import worker finished.")
