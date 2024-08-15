from datetime import datetime
from typing import Optional

import pytest

from src.app.core.models.collection_file_change import CollectionFileChange


DOWNLOADED_AT = datetime(2024, 1, 1)
IMPORTED_AT = datetime(2024, 2, 1)


test_cases_str = [
    # downloaded_at, imported_at, download_error
    pytest.param(None, None, None, id="none_none_none"),
    pytest.param(DOWNLOADED_AT, None, None, id="set_none_none"),
    pytest.param(None, IMPORTED_AT, None, id="none_set_none"),
    pytest.param(None, None, True, id="none_none_true"),
    pytest.param(None, None, False, id="none_none_false"),
    pytest.param(DOWNLOADED_AT, IMPORTED_AT, None, id="set_set_none"),
    pytest.param(DOWNLOADED_AT, None, None, id="set_none_true"),
    pytest.param(DOWNLOADED_AT, None, None, id="set_none_set"),
    pytest.param(None, IMPORTED_AT, None, id="none_set_true"),
    pytest.param(None, IMPORTED_AT, None, id="none_set_false"),
    pytest.param(DOWNLOADED_AT, IMPORTED_AT, None, id="set_set_true"),
    pytest.param(DOWNLOADED_AT, IMPORTED_AT, None, id="set_set_false"),
]
"""downloaded_at: Optional[datetime], imported_at: Optional[datetime], download_error: Optional[bool]"""


@pytest.mark.parametrize(["downloaded_at", "imported_at", "download_error"], test_cases_str)
def test_downloaded_at_in_str(downloaded_at: Optional[datetime], imported_at: Optional[datetime], download_error: Optional[bool]):
    string = str(CollectionFileChange(downloaded_at=downloaded_at, imported_at=imported_at, download_error=download_error))

    if downloaded_at:
        assert f"downloaded_at={downloaded_at.isoformat()}" in string
    if imported_at:
        assert f"imported_at={imported_at.isoformat()}" in string
    if download_error is not None:
        assert f"download_error={download_error}" in string
