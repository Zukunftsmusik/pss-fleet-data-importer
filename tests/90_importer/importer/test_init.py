import logging
import queue

from src.app.core.config import Config
from src.app.importer import Importer
from src.app.models import ImportStatus


def test_init(configuration: Config):
    importer = Importer(
        configuration,
        "database",
        "gdrive_client",
        "pss_fleet_data_client",
    )

    assert id(importer.config) == id(configuration)
    assert importer.database == "database"
    assert importer.gdrive_client == "gdrive_client"
    assert importer.fleet_data_client == "pss_fleet_data_client"
    assert isinstance(importer.status, ImportStatus)
    assert isinstance(importer.import_queue, queue.Queue)
    assert isinstance(importer.database_queue, queue.Queue)
    assert isinstance(importer.logger, logging.Logger)
    assert id(importer.logger) != id(configuration.logger)
    assert id(importer.logger.parent) == id(configuration.logger)
