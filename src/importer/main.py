import asyncio
import sys

from .core import Importer, get_config
from .database import DATABASE


def main():
    asyncio.run(do_import())


async def do_import():
    print("===========================")
    print("  PSS FLEET DATA IMPORTER")
    print("===========================")
    print()
    print(f"Version: {get_config().app_version}")
    print(f"Log level: {get_config().debug_mode}")
    try:
        importer = Importer(
            get_config(),
            DATABASE,
            gdrive_folder_id=get_config().gdrive_folder_id,
            api_server_url=get_config().api_default_server_url,
            api_key=get_config().api_key,
            download_thread_pool_size=get_config().download_thread_pool_size,
            temp_download_folder=get_config().temp_download_folder,
        )

        # await importer.run_import_loop(modified_before=datetime(2020, 8, 1))
        # await importer.run_import_loop(modified_before=datetime(2022, 1, 1))
        await importer.run_import_loop()
    except KeyboardInterrupt:
        get_config().logger.warn("\nAborted by user, exiting.")
        sys.exit(1)


if __name__ == "__main__":
    main()
