import pytest

from tests.fake_classes import FakeConfig


@pytest.fixture(scope="function")
def mock_config() -> FakeConfig:
    return FakeConfig()
