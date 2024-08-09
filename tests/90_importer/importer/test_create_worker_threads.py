import threading

from src.app.importer.importer import Importer


def test(importer: Importer):
    threads = importer.create_worker_threads([])
    assert threads
    assert isinstance(threads, list)
    assert len(threads) == 3

    for thread in threads:
        assert isinstance(thread, threading.Thread)
        assert thread.name
        assert "worker" in thread.name.lower()
        assert thread.daemon is True

    for thread_1, thread_2 in zip(threads[1:], threads[:-1]):
        assert id(thread_1) != id(thread_2)
        assert thread_1.name != thread_2.name
        assert thread_1._target != thread_2._target
