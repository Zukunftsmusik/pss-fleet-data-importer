from typing import Optional

import pytest

from src.app.core.models.collection_file_change import CollectionFileChange


test_cases_repr = [
    # imported, error
    pytest.param(None, None, id="none_none"),
    pytest.param(None, True, id="none_true"),
    pytest.param(None, False, id="none_false"),
    pytest.param(True, None, id="true_none"),
    pytest.param(True, True, id="true_true"),
    pytest.param(True, False, id="true_false"),
    pytest.param(False, None, id="false_none"),
    pytest.param(False, True, id="false_true"),
    pytest.param(False, False, id="false_false"),
]
"""imported: Optional[bool], error: Optional[bool]"""


@pytest.mark.parametrize(["imported", "error"], test_cases_repr)
def test_downloaded_at_in_str(imported: Optional[bool], error: Optional[bool]):
    string = repr(CollectionFileChange(imported=imported, error=error))

    assert string.startswith(f"<{CollectionFileChange.__name__}")
    assert string.endswith(">")

    assert f"imported={imported}" in string
    assert f"error={error}" in string
