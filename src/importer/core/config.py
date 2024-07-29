import logging
import logging.config
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from . import logger


@dataclass(frozen=True)
class Config:
    # Basic settings
    app_name: str = "importer"
    pss_start_date: datetime = datetime(2016, 1, 6, tzinfo=timezone.utc)
    earliest_data_date: datetime = datetime(2019, 10, 10, tzinfo=timezone.utc)
    temp_download_folder: Path = Path("./downloads")
    download_thread_pool_size: int = 4

    # PSS Fleet Data API
    api_default_server_url: str = os.getenv("FLEET_DATA_API_URL", "https://fleetdata.dolores2.xyz")
    api_key: Optional[str] = os.getenv("FLEET_DATA_API_KEY")

    # Google Drive
    gdrive_project_id: str = os.getenv("GDRIVE_SERVICE_PROJECT_ID")
    gdrive_private_key_id: str = os.getenv("GDRIVE_SERVICE_PRIVATE_KEY_ID")
    gdrive_private_key: str = os.getenv("GDRIVE_SERVICE_PRIVATE_KEY")
    gdrive_client_email: str = os.getenv("GDRIVE_SERVICE_CLIENT_EMAIL")
    gdrive_client_id: str = os.getenv("GDRIVE_SERVICE_CLIENT_ID")
    gdrive_scopes: list[str] = field(default_factory=lambda: ["https://www.googleapis.com/auth/drive"])
    gdrive_folder_id: str = os.getenv("GDRIVE_FOLDER_ID", "10wOZgAQk_0St2Y_jC3UW497LVpBNxWmP")
    gdrive_service_account_file_path: str = "client_secrets.json"
    gdrive_settings_file_path: str = "settings.yaml"

    # Flags
    debug_mode: bool = os.getenv("DEBUG_MODE", "false").lower() == "true"
    in_github_actions: bool = os.getenv("GITHUB_ACTIONS", "false").lower() == "true"  # True if in github actions
    keep_downloaded_files: bool = os.getenv("KEEP_DOWNLOADED_FILES", "false").lower() == "true"
    reinitialize_database_on_startup: bool = os.getenv("REINITIALIZE_DATABASE", "false").lower() == "true"

    # Database
    db_engine_echo: bool = os.getenv("DATABASE_ENGINE_ECHO", "false") == "true"
    db_name: str = os.getenv("DATABASE_NAME", "pss-fleet-data-importer")
    db_url: str = os.getenv("DATABASE_URL")

    @property
    def db_async_connection_str(self) -> str:
        return f"postgresql+asyncpg://{self.db_url}/{self.db_name}"

    @property
    def db_sync_connection_str(self) -> str:
        return f"postgresql://{self.db_url}/{self.db_name}"

    @property
    def db_server_and_port(self) -> str:
        return self.db_url.split("@")[1]

    @property
    def log_level(self) -> int:
        if self.debug_mode:
            return logging.DEBUG
        return logging.INFO

    @property
    def logger(self) -> logging.Logger:
        result = logging.getLogger(self.app_name)
        result.setLevel(self.log_level)
        return result

    def configure_logging(self, config: Optional[dict] = None):
        config = config or LOGGING_BASE_CONFIG
        config = dict(config)
        logging.config.dictConfig(config)
        logging.Formatter.converter = time.gmtime


CONFIG = Config()


LOGGING_BASE_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(levelname)-8.8s  [%(name)s]  %(message)s",
        },
        "standard_with_time": {
            "format": "%(asctime)s  %(levelname)-8.8s  [%(name)s]  %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "alembic_generic": {
            "format": "%(levelname)-8.8s  [%(name)s]  %(message)s",
            "datefmt": "%H:%M:%S",
        },
    },
    "filters": {
        "remove_src": {"()": logger.RemoveSrcFromLoggerNameFilter},
    },
    "handlers": {
        "alembic_console": {
            "level": logging.NOTSET,
            "formatter": "alembic_generic",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
        },
        "nullhandler": {
            "level": logging.DEBUG,
            "class": "logging.NullHandler",
        },
        "stderr": {
            "level": logging.NOTSET,
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
            "filters": ["remove_src"],
        },
        "stderr_with_time": {
            "level": logging.NOTSET,
            "formatter": "standard_with_time",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
            "filters": ["remove_src"],
        },
    },
    "loggers": {
        "importer": {
            "handlers": ["stderr"],
            "level": CONFIG.log_level,
            "propagate": False,
        },
        "": {  # root logger
            "handlers": ["alembic_console"],
            "level": logging.WARN,
        },
        "sqlalchemy.engine.Engine": {
            "handlers": ["nullhandler"],  # Suppress logging to prevent duplication of log messages
            "level": logging.WARN,
            "propagate": False,
        },
        "sqlalchemy.engine": {
            "handlers": ["alembic_console"],
            "level": logging.WARN,
            "propagate": False,
        },
        "alembic": {
            "handlers": ["alembic_console"],
            "level": logging.INFO,
            "propagate": False,
        },
    },
}


CONFIG.configure_logging(LOGGING_BASE_CONFIG)
