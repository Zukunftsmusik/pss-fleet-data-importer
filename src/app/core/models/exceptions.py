from typing import Optional


class ImporterBaseError(Exception):
    def __init__(self, message: str, inner_exception: Optional[Exception] = None):
        self.inner_exception: Optional[Exception] = inner_exception
        self.message: str = message
        super().__init__(self.message)

    def __str__(self) -> str:
        return self.message

    def __repr__(self) -> str:
        return f"<{ImporterBaseError.__name__} message={self.message}, inner_exception={type(self.inner_exception)}>"


class DownloadFailedError(ImporterBaseError):
    def __init__(self, file_name: str, reason: str, inner_exception: Optional[Exception] = None):
        self.file_name: str = file_name
        self.reason: str = reason
        message = f"Download of file '{file_name}' failed: {reason}"
        super().__init__(message, inner_exception=inner_exception)

    def __repr__(self) -> str:
        return f"<{DownloadFailedError.__name__} file_name={self.file_name}, reason={self.reason}, inner_exception={type(self.inner_exception)}>"


class OperationCancelledError(Exception):
    pass


__all__ = [
    DownloadFailedError.__name__,
    ImporterBaseError.__name__,
    OperationCancelledError.__name__,
]
