from . import core, database, models


__app_name__ = core.config.get_config().app_name
__version__ = core.config.get_config().app_version


__all__ = [
    core.__name__,
    database.__name__,
    models.__name__,
]
