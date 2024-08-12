from contextlib import contextmanager

from .. import LOGGER_BASE


LOGGER = LOGGER_BASE.getChild("Database")


@contextmanager
def database_create(database_name: str):
    LOGGER.info("Creating database `%s`.", database_name)
    yield
    LOGGER.info("Database `%s` created.", database_name)


def database_migrate(database_name: str):
    LOGGER.info("Applying migrations to database `%s`.", database_name)


@contextmanager
def database_reinitialize(database_name: str):
    LOGGER.info("Dropping tables of database `%s` as requested.", database_name)
    yield
    LOGGER.info("Tables of database `%s` dropped.", database_name)


def database_updated(database_name: str):
    LOGGER.info("Database `%s` schema is up-to-date.", database_name)


def setup_async_engine(async_connection_string: str):
    LOGGER.info("Setting up async database engine for: %s", async_connection_string.split("@")[-1])


def transaction_error(exception: Exception):
    LOGGER.error("An error occured during a database transaction.", exc_info=exception, stack_info=True)
