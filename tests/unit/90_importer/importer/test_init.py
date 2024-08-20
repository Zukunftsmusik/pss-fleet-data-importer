from fake_classes import FakeConfig
from src.app.importer import Importer
from src.app.models import ImportStatus


def test_init(fake_config: FakeConfig):
    importer = Importer(
        fake_config,
        "gdrive_client",
        "pss_fleet_data_client",
    )

    assert id(importer.config) == id(fake_config)
    assert importer.gdrive_client == "gdrive_client"
    assert importer.fleet_data_client == "pss_fleet_data_client"
    assert isinstance(importer.status, ImportStatus)
