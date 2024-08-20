from src.app.core.models.status import StatusFlag


def test_bool_magic_returns_value(status_flag_false: StatusFlag):
    assert bool(status_flag_false) == status_flag_false.value

    status_flag_false.value = True
    assert bool(status_flag_false) == status_flag_false.value
    assert bool(status_flag_false) is True

    status_flag_false.value = False
    assert bool(status_flag_false) == status_flag_false.value
    assert bool(status_flag_false) is False


def test_property_value(status_flag_false: StatusFlag):
    assert status_flag_false.value == status_flag_false._StatusFlag__value
    assert status_flag_false.value is False

    status_flag_false.value = True
    assert status_flag_false.value == status_flag_false._StatusFlag__value
    assert status_flag_false.value is True

    status_flag_false.value = False
    assert status_flag_false.value == status_flag_false._StatusFlag__value
    assert status_flag_false.value is False
