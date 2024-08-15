from typing import Optional

import pytest

from src.app.core.models.base_error import ImporterBaseError


test_cases_str = [
    # message, inner_exception
    pytest.param(None, None, id="none_none"),
    pytest.param("Message", None, id="message_none"),
    pytest.param(None, ValueError(), id="none_exception"),
    pytest.param("Message", ValueError(), id="message_exception"),
]
"""message: Optional[str], inner_exception: Optional[Exception]"""


@pytest.mark.parametrize(["message", "inner_exception"], test_cases_str)
def test_str(message: Optional[str], inner_exception: Optional[Exception]):
    string = str(ImporterBaseError(message, inner_exception=inner_exception))

    assert string.startswith(ImporterBaseError.__name__)
    assert str(message) in string
