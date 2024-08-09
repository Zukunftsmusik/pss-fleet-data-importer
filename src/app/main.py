import asyncio
import logging
import sys
from datetime import datetime  # noqa

from pss_fleet_data import PssFleetDataClient

from .core import config
from .core.gdrive import GoogleDriveClient
from .database import db
from .importer import Importer


async def main():
    configuration = config.get_config()
    configuration.configure_logging(config.get_logging_base_config(configuration))

    print()
    print("  ===========================")
    print("    PSS FLEET DATA IMPORTER")
    print("  ===========================")
    print()
    print(f"  Version: {configuration.app_version}")
    print(f"  Log level: {logging.getLevelName(configuration.log_level)}")
    print(f"  Debug mode: {configuration.debug_mode}")
    print(f"  API server URL: {configuration.api_default_server_url}")
    print(f"  Google Drive folder ID: {configuration.gdrive_folder_id}")
    print(f"  Download folder: {configuration.temp_download_folder}")
    print(f"  Download thread pool size: {configuration.download_thread_pool_size}")
    print()

    database = db.get_db()

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
        logger=configuration.logger,
    )
    gdrive_client.initialize()

    pss_fleet_data_client = PssFleetDataClient(configuration.api_default_server_url, configuration.api_key)

    importer = Importer(
        configuration,
        database,
        gdrive_client,
        pss_fleet_data_client,
    )

    if configuration.log_level <= logging.INFO:
        print()
    print("  Starting import loop.")

    if await importer.check_api_server_connection():
        try:
            await importer.run_import_loop(modified_before=datetime(2019, 10, 20))
            # asyncio.run(importer.run_import_loop(modified_after=datetime(2021, 11, 11), modified_before=datetime(2021, 11, 12)))
            # asyncio.run(importer.run_import_loop())
        except KeyboardInterrupt:
            configuration.logger.warn("\nAborted by user, shutting down.")
            importer.cancel_workers()
            sys.exit(1)
    else:
        configuration.logger.critical("Cannot connect to server at: %s", pss_fleet_data_client.base_url)


if __name__ == "__main__":
    asyncio.run(main())
