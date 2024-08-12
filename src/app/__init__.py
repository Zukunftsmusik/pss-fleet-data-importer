from . import core, database, importer, models


__app_name__ = core.config.ConfigRepository.get_config().app_name
__version__ = core.config.ConfigRepository.get_config().app_version


__all__ = [
    core.__name__,
    database.__name__,
    importer.__name__,
    models.__name__,
]
