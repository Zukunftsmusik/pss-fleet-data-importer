import asyncio
import sys

from .core import CONFIG, Importer
from .database import DATABASE


def main():
    asyncio.run(do_import())


async def do_import():
    try:
        importer = Importer(
            CONFIG,
            DATABASE,
            gdrive_folder_id=CONFIG.gdrive_folder_id,
            api_server_url=CONFIG.api_default_server_url,
            api_key=CONFIG.api_key,
            download_thread_pool_size=CONFIG.download_thread_pool_size,
            temp_download_folder=CONFIG.temp_download_folder,
        )

        # await importer.run_import_loop(modified_before=datetime(2020, 8, 1))
        # await importer.run_import_loop(modified_before=datetime(2022, 1, 1))
        await importer.run_import_loop()
    except KeyboardInterrupt:
        CONFIG.logger.warn("\nAborted by user, exiting.")
        sys.exit(1)


if __name__ == "__main__":
    main()
