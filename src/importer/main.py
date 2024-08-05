import asyncio
import logging
import sys
from datetime import datetime

from .core import Importer, get_config
from .database import get_db


def main():
    print()
    print("  ===========================")
    print("    PSS FLEET DATA IMPORTER")
    print("  ===========================")
    print()
    print(f"  Version: {get_config().app_version}")
    print(f"  Log level: {logging.getLevelName(get_config().log_level)}")
    print(f"  Debug mode: {get_config().debug_mode}")
    print(f"  API server URL: {get_config().api_default_server_url}")
    print(f"  Google Drive folder ID: {get_config().gdrive_folder_id}")
    print(f"  Download folder: {get_config().temp_download_folder}")
    print(f"  Download thread pool size: {get_config().download_thread_pool_size}")
    print()
    importer = Importer(
        get_config(),
        get_db(),
        gdrive_folder_id=get_config().gdrive_folder_id,
        api_server_url=get_config().api_default_server_url,
        api_key=get_config().api_key,
        download_thread_pool_size=get_config().download_thread_pool_size,
        temp_download_folder=get_config().temp_download_folder,
    )

    if get_config().log_level <= logging.INFO:
        print()
    print("  Starting import loop.")

    try:
        asyncio.run(importer.run_import_loop(modified_before=datetime(2019, 10, 20)))
        # await importer.run_import_loop(modified_before=datetime(2022, 1, 1))
        # await importer.run_import_loop()
    except KeyboardInterrupt:
        get_config().logger.warn("\nAborted by user, shutting down.")
        importer.cancel_workers()
        sys.exit(1)


if __name__ == "__main__":
    main()
