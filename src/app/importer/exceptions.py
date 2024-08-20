from typing import Optional

from ..core.models.base_error import ImporterBaseError


class DownloadFailedError(ImporterBaseError):
    def __init__(self, file_name: str, reason: str, inner_exception: Optional[Exception] = None):
        self.file_name: str = file_name
        self.reason: str = reason
        message = f"Download of file '{file_name}' failed: {reason}"
        super().__init__(message, inner_exception=inner_exception)

    def __repr__(self) -> str:
        return f"<{DownloadFailedError.__name__} file_name={self.file_name}, reason={self.reason}, inner_exception={type(self.inner_exception)}>"


__all__ = [
    DownloadFailedError.__name__,
]
