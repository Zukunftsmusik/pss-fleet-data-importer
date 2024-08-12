import logging
import logging.config
import time
from pathlib import Path
from typing import Optional, Union

from httpx import URL

from ..core.config import Config
from . import LOGGER_BASE, filter


def aborted():
    LOGGER_BASE.warn("\nAborted by user, shutting down.")


def configure_logging(logging_config: dict, log_folder_path: Optional[Union[Path, str]]):
    logging.config.dictConfig(dict(logging_config))
    logging.Formatter.converter = time.gmtime
    if log_folder_path:
        log_folder_path.mkdir(parents=True, exist_ok=True)


def configure_logging_from_app_config(app_config: Config):
    configure_logging(get_logging_base_config(app_config), app_config.log_folder_path)


def connection_failure(api_url: Union[URL, str]):
    LOGGER_BASE.critical("Cannot connect to server at: %s", api_url)


def get_logging_base_config(config: Config):
    logging_base_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": "%(levelname)-8.8s  [%(name)s]  %(message)s",
            },
            "standard_with_time": {
                "format": "%(asctime)s  %(levelname)-8.8s  [%(name)s] %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "standard_file": {
                "format": "%(levelname)-8.8s  [%(name)s] (%(thread)-8.8d)  %(message)s",
            },
            "standard_file_with_time": {
                "format": "%(asctime)s  %(levelname)-8.8s  [%(name)s] (%(thread)-8.8d)  %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "alembic_generic": {
                "format": "%(levelname)-8.8s  [%(name)s]  %(message)s",
                "datefmt": "%H:%M:%S",
            },
        },
        "filters": {
            "remove_src": {"()": filter.RemoveSrcFromLoggerNameFilter},
        },
        "handlers": {
            "alembic_console": {
                "level": logging.NOTSET,
                "formatter": "alembic_generic",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stderr",
            },
            "nullhandler": {
                "level": logging.DEBUG,
                "class": "logging.NullHandler",
            },
            "stderr": {
                "level": logging.NOTSET,
                "formatter": "standard",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stderr",
                "filters": ["remove_src"],
            },
            "stderr_with_time": {
                "level": logging.NOTSET,
                "formatter": "standard_with_time",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stderr",
                "filters": ["remove_src"],
            },
        },
        "loggers": {
            "importer": {
                "handlers": ["stderr", "log_file"] if config.log_file_path else ["stderr"],
                "level": config.app_log_level,
                "propagate": False,
            },
            "": {  # root logger
                "handlers": ["alembic_console"],
                "level": logging.WARN,
            },
            "sqlalchemy.engine.Engine": {
                "handlers": ["nullhandler"],  # Suppress logging to prevent duplication of log messages
                "level": logging.WARN,
                "propagate": False,
            },
            "sqlalchemy.engine": {
                "handlers": ["alembic_console"],
                "level": logging.WARN,
                "propagate": False,
            },
            "alembic": {
                "handlers": ["alembic_console"],
                "level": max(logging.INFO, config.app_log_level),
                "propagate": False,
            },
        },
    }

    if config.log_file_path:
        logging_base_config["handlers"]["log_file"] = {
            "level": logging.DEBUG,
            "formatter": "standard_file_with_time",
            "class": "logging.FileHandler",
            "filename": config.log_file_path,
            "delay": True,
            "filters": ["remove_src"],
        }

    return logging_base_config


__all__ = [
    get_logging_base_config.__name__,
]
