import asyncio
import logging
import sys
from datetime import datetime  # noqa

from pss_fleet_data import PssFleetDataClient

from ..app import __version__
from .core import config
from .core.gdrive import GoogleDriveClient
from .importer import Importer
from .log import base as logger_base


async def main():
    configuration = config.ConfigRepository.get_config()
    logger_base.configure_logging_from_app_config(configuration)

    print()
    print("  ===========================")
    print("    PSS FLEET DATA IMPORTER")
    print("  ===========================")
    print()
    print(f"  Version: {__version__}")
    print(f"  Log level: {logging.getLevelName(configuration.app_log_level)}")
    print(f"  Debug mode: {configuration.debug_mode}")
    print(f"  API server URL: {configuration.api_default_server_url}")
    print(f"  Google Drive folder ID: {configuration.gdrive_folder_id}")
    print(f"  Download folder: {configuration.temp_download_folder}")
    print(f"  Download thread pool size: {configuration.download_thread_pool_size}")
    print()

    gdrive_client = GoogleDriveClient(
        configuration.gdrive_project_id,
        configuration.gdrive_private_key_id,
        configuration.gdrive_private_key,
        configuration.gdrive_client_email,
        configuration.gdrive_client_id,
        configuration.gdrive_scopes,
        configuration.gdrive_folder_id,
        configuration.gdrive_service_account_file_path,
        configuration.gdrive_settings_file_path,
    )
    gdrive_client.initialize()

    pss_fleet_data_client = PssFleetDataClient(configuration.api_default_server_url, configuration.api_key)

    importer = Importer(
        configuration,
        pss_fleet_data_client,
    )

    if configuration.app_log_level <= logging.INFO:
        print()
    print("  Starting import loop.")

    if await importer.check_api_server_connection():
        try:
            # await importer.run_import_loop(modified_after=datetime(2024, 8, 16, 12), modified_before=datetime(2024, 8, 16, 13))
            # await importer.run_import_loop(modified_after=datetime(2024, 8, 20, 10))
            # await importer.run_import_loop(run_once=True, modified_before=datetime(2022, 4, 1))  # Including 2 months of schema version 9
            # await importer.run_import_loop(run_once=True, modified_before=datetime(2019, 10, 20))
            await importer.run_import_loop()
        except KeyboardInterrupt:
            logger_base.aborted()
            importer.cancel_workers()
            sys.exit(1)
    else:
        logger_base.connection_failure(pss_fleet_data_client.base_url)


if __name__ == "__main__":
    asyncio.run(main())
