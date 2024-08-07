import asyncio
import logging
import sys

from .core import config
from .core.importer import Importer
from .database import db


def main():
    print()
    print("  ===========================")
    print("    PSS FLEET DATA IMPORTER")
    print("  ===========================")
    print()
    print(f"  Version: {config.get_config().app_version}")
    print(f"  Log level: {logging.getLevelName(config.get_config().log_level)}")
    print(f"  Debug mode: {config.get_config().debug_mode}")
    print(f"  API server URL: {config.get_config().api_default_server_url}")
    print(f"  Google Drive folder ID: {config.get_config().gdrive_folder_id}")
    print(f"  Download folder: {config.get_config().temp_download_folder}")
    print(f"  Download thread pool size: {config.get_config().download_thread_pool_size}")
    print()
    importer = Importer(
        config.get_config(),
        db.get_db(),
        gdrive_folder_id=config.get_config().gdrive_folder_id,
        api_server_url=config.get_config().api_default_server_url,
        api_key=config.get_config().api_key,
        download_thread_pool_size=config.get_config().download_thread_pool_size,
        temp_download_folder=config.get_config().temp_download_folder,
    )

    if config.get_config().log_level <= logging.INFO:
        print()
    print("  Starting import loop.")

    try:
        # asyncio.run(importer.run_import_loop(modified_before=datetime(2019, 11, 1)))
        # asyncio.run(importer.run_import_loop(modified_after=datetime(2022, 1, 28), modified_before=datetime(2022, 1, 29)))
        asyncio.run(importer.run_import_loop())
    except KeyboardInterrupt:
        config.get_config().logger.warn("\nAborted by user, shutting down.")
        importer.cancel_workers()
        sys.exit(1)


if __name__ == "__main__":
    main()
