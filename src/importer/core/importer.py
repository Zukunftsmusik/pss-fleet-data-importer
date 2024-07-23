import logging
import queue

from ..database import DATABASE
from . import CONFIG


class Importer:
    def __init__(self, *, gdrive_folder_id: str = None, api_server_url: str = None):
        self.__gdrive_folder_id = gdrive_folder_id or CONFIG.default_gdrive_folder_id
        self.__api_server_url = api_server_url or CONFIG.default_api_server_url

        self.__logger = CONFIG.logger.getChild(Importer.__name__)
        self.__download_queue = queue.Queue()
        self.__import_queue = queue.Queue()

        DATABASE.initialize_database(
            CONFIG.db_sync_connection_str,
            CONFIG.db_async_connection_str,
            CONFIG.debug_mode,
            CONFIG.reinitialize_database_on_startup,
        )

    @property
    def api_server_url(self) -> str:
        return self.__api_server_url

    @property
    def gdrive_folder_id(self) -> str:
        return self.__gdrive_folder_id

    @property
    def logger(self) -> logging.Logger:
        return self.__logger

    def start_downloads(self):
        raise NotImplementedError()

    def start_imports(self):
        raise NotImplementedError()

    def run_bulk_import(self):
        self.start_downloads()
        self.start_imports()
        self.__download_queue.join()
        self.__import_queue.join()

    def start_import_loop(self):
        raise NotImplementedError()
