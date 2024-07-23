from .core import CONFIG, Importer


def main():
    importer = Importer(
        gdrive_folder_id=CONFIG.default_gdrive_folder_id,
        api_server_url=CONFIG.default_api_server_url,
    )
    importer.run_bulk_import()
    importer.start_import_loop()


if __name__ == "__main__":
    main()
