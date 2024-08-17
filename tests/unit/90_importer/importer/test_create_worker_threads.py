import threading

from src.app.importer.importer import Importer


def test(importer: Importer):
    thread = importer.create_download_worker_thread([])
    assert thread
    assert isinstance(thread, threading.Thread)
    assert thread.name
    assert "worker" in thread.name.lower()
    assert thread.daemon is True
