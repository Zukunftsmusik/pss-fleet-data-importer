from src.app.core.config import ConfigRepository


def test_multiple_calls_return_same_config():
    config_1 = ConfigRepository.get_config()
    config_2 = ConfigRepository.get_config()
    config_3 = ConfigRepository.get_config()

    assert id(config_1) == id(config_2) == id(config_3)
