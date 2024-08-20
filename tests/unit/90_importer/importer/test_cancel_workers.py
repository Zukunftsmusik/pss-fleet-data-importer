import logging

import pytest

from src.app.importer import Importer


def test_cancel_workers(fake_importer: Importer, caplog: pytest.LogCaptureFixture):
    with caplog.at_level(logging.WARN):
        fake_importer.cancel_workers()
    assert "Cancelling workers" in caplog.text
    assert fake_importer.status.cancelled
