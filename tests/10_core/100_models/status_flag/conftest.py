import pytest

from src.app.core.models.status import StatusFlag


@pytest.fixture(scope="function")
def status_flag_false() -> StatusFlag:
    return StatusFlag("false", False)


@pytest.fixture(scope="function")
def status_flag_true() -> StatusFlag:
    return StatusFlag("true", True)
