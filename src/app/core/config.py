import logging
import logging.config
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, dataclass_transform


@dataclass_transform()
class ConfigBase:
    # Basic settings
    pss_start_date: datetime = datetime(2016, 1, 6, tzinfo=timezone.utc)
    earliest_data_date: datetime = datetime(2019, 10, 10, tzinfo=timezone.utc)
    temp_download_folder: Path = Path("./downloads")
    download_thread_pool_size: int = int(os.getenv("FLEET_DATA_IMPORTER_WORKER_COUNT", 2))
    log_folder: Optional[str] = os.getenv("LOG_FOLDER_PATH")
    log_level: Optional[str] = os.getenv("LOG_LEVEL")

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
    def app_log_level(self) -> int:
        log_level = None
        if self.log_level:
            log_level = logging.getLevelNamesMapping().get(self.log_level)
            if log_level:
                return log_level

        if self.debug_mode:
            return logging.DEBUG

        return logging.INFO

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
    def log_file_name(self) -> Optional[str]:
        return "pss_fleet_data_importer_" + datetime.now(tz=timezone.utc).strftime("%Y%m%d-%H%M%S") + ".log"

    @property
    def log_file_path(self) -> Optional[Path]:
        if self.log_folder_path:
            return self.log_folder_path.joinpath(self.log_file_name)
        return None

    @property
    def log_folder_path(self) -> Optional[Path]:
        if self.log_folder:
            return Path(self.log_folder)
        return None


@dataclass(frozen=True)
class Config(ConfigBase):
    pass


class ConfigRepository:
    __config: Config = None

    @classmethod
    def get_config(cls) -> Config:
        if not cls.__config:
            cls.__config = Config()
        return cls.__config


__all__ = [
    # Classes
    Config.__name__,
    ConfigRepository.__name__,
]
