from fake_classes import FakeImporter, create_fake_gdrive_file


async def test_happy_path(fake_importer: FakeImporter):
    for _ in range(10):
        fake_importer.gdrive_client.files.append(create_fake_gdrive_file())

    await fake_importer.run_bulk_import()
