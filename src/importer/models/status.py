from threading import Lock

from cancel_token import CancellationToken


class StatusFlag:
    def __init__(self, name: str, initial_value: bool):
        self.__value: bool = initial_value
        self.name = name
        self.__lock = Lock()

    def __bool__(self) -> bool:
        return self.value

    def __nonzero__(self) -> bool:
        return self.value

    @property
    def value(self) -> bool:
        with self.__lock:
            return self.__value

    @value.setter
    def value(self, new_value: bool):
        with self.__lock:
            self.__value = new_value


class ImportStatus:
    def __init__(self):
        self.bulk_download_running = StatusFlag("bulk_download_running", False)
        self.bulk_import_running = StatusFlag("bulk_import_running", False)
        self.cancel_token: CancellationToken = CancellationToken()

    @property
    def cancelled(self) -> bool:
        return self.cancel_token.cancelled


__all__ = [
    ImportStatus.__name__,
    StatusFlag.__name__,
]
