import logging

import pytest


@pytest.fixture(scope="function")
def logger() -> logging.Logger:
    logging.basicConfig(level=0)
    return logging.getLogger("pytest")
