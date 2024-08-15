import logging
from pathlib import Path
from typing import Optional

import pytest

from tests.fake_classes import FakeConfig


def test_async_connection_string_well_formed(mock_config: FakeConfig):
    assert mock_config.db_async_connection_str.startswith("postgresql+asyncpg://")
    assert mock_config.db_url in mock_config.db_async_connection_str
    assert mock_config.db_name in mock_config.db_async_connection_str


def test_sync_connection_string_well_formed(mock_config: FakeConfig):
    assert mock_config.db_sync_connection_str.startswith("postgresql://")
    assert mock_config.db_url in mock_config.db_sync_connection_str
    assert mock_config.db_name in mock_config.db_sync_connection_str


def test_db_server_and_port_correct(mock_config: FakeConfig):
    mock_config.db_url = "user:password@127.0.0.1:8000"
    assert mock_config.db_server_and_port == "127.0.0.1:8000"


def test_log_file_name_well_formed(mock_config: FakeConfig):
    log_file_name = mock_config.log_file_name

    assert isinstance(log_file_name, str)
    assert log_file_name.startswith("pss_fleet_data_importer_")
    assert log_file_name.endswith(".log")
    assert len(log_file_name) == len("pss_fleet_data_importer_") + len(".log") + len("20240101-235900")


def test_log_folder_path_is_set(mock_config: FakeConfig):
    mock_path = "/dev/null"
    mock_config.log_folder = mock_path

    assert isinstance(mock_config.log_folder_path, Path)
    assert str(mock_config.log_folder_path) == mock_path
    assert isinstance(mock_config.log_file_path, Path)
    assert len(str(mock_config.log_file_path)) == len(mock_path) + 1 + len("pss_fleet_data_importer_") + len(".log") + len("20240101-235900")


def test_log_folder_path_not_set(mock_config: FakeConfig):
    mock_config.log_folder = None

    assert mock_config.log_folder_path is None
    assert mock_config.log_file_path is None


test_cases_log_level = [
    # from_env, debug_mode, expected_result
    pytest.param("DEBUG", False, logging.DEBUG, id="debug_false"),
    pytest.param("INFO", False, logging.INFO, id="info_false"),
    pytest.param("WARN", False, logging.WARN, id="warn_false"),
    pytest.param("DEBUG", True, logging.DEBUG, id="debug_true"),
    pytest.param("INFO", True, logging.INFO, id="info_true"),
    pytest.param("WARN", True, logging.WARN, id="warn_true"),
    pytest.param("WRONG", False, logging.INFO, id="wrong_false"),
    pytest.param("WRONG", True, logging.DEBUG, id="wrong_true"),
    pytest.param(None, False, logging.INFO, id="none_false"),
    pytest.param(None, True, logging.DEBUG, id="none_true"),
]


@pytest.mark.parametrize(["from_env", "debug_mode", "expected_result"], test_cases_log_level)
def test_log_level_correct(mock_config: FakeConfig, from_env: Optional[str], debug_mode: bool, expected_result: int):
    mock_config.log_level = from_env
    mock_config.debug_mode = debug_mode

    assert mock_config.app_log_level == expected_result
