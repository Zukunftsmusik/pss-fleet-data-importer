from threading import Lock

from .cancellation_token import CancellationToken


class StatusFlag:
    def __init__(self, name: str, initial_value: bool):
        self.__value: bool = initial_value
        self.name = name
        self.__value_lock = Lock()

    def __bool__(self) -> bool:
        return self.value

    def __str__(self) -> str:
        return f"StatusFlag({self.name})={self.value}"

    def __repr__(self) -> str:
        return f"<StatusFlag name={self.name}, value={self.__value}>"

    @property
    def value(self) -> bool:
        with self.__value_lock:
            return self.__value

    @value.setter
    def value(self, new_value: bool):
        with self.__value_lock:
            self.__value = new_value


class ImportStatus:
    def __init__(self):
        self.bulk_download_running = StatusFlag("bulk_download_running", False)
        self.bulk_database_running = StatusFlag("bulk_database_running", False)
        self.cancel_token: CancellationToken = CancellationToken()
        self.download_worker_timed_out = StatusFlag("download_worker_timed_out", False)

    @property
    def cancelled(self) -> bool:
        return self.cancel_token.cancelled


__all__ = [
    ImportStatus.__name__,
    StatusFlag.__name__,
]
