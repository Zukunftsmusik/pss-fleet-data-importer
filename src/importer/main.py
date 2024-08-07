import asyncio
import logging
import sys

from .core import config
from .core.importer import Importer
from .database import db


def main():
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
    importer = Importer(
        configuration,
        db.get_db(),
        gdrive_folder_id=configuration.gdrive_folder_id,
        api_server_url=configuration.api_default_server_url,
        api_key=configuration.api_key,
        download_thread_pool_size=configuration.download_thread_pool_size,
        temp_download_folder=configuration.temp_download_folder,
    )

    if configuration.log_level <= logging.INFO:
        print()
    print("  Starting import loop.")

    try:
        # asyncio.run(importer.run_import_loop(modified_before=datetime(2019, 11, 1)))
        # asyncio.run(importer.run_import_loop(modified_after=datetime(2022, 1, 28), modified_before=datetime(2022, 1, 29)))
        asyncio.run(importer.run_import_loop())
    except KeyboardInterrupt:
        configuration.logger.warn("\nAborted by user, shutting down.")
        importer.cancel_workers()
        sys.exit(1)


if __name__ == "__main__":
    main()
