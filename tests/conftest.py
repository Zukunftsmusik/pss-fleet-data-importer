import logging

import pytest


@pytest.fixture
def logger() -> logging.Logger:
    return logging.getLogger("pytest")
