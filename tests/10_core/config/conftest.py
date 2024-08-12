import pytest

from mock_classes import MockConfig


@pytest.fixture(scope="function")
def mock_config() -> MockConfig:
    return MockConfig()
