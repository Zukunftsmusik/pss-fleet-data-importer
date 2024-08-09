from typing import Optional


class DownloadFailedError(Exception):
    def __init__(self, file_name: str, reason: str, inner_exception: Optional[Exception] = None):
        self.file_name: str = file_name
        self.inner_exception: Optional[Exception] = inner_exception
        self.message = f"Download of file '{file_name}' failed: {reason}"
        self.reason: str = reason
        super().__init__(self.message)

    def __str__(self) -> str:
        return self.message

    def __repr__(self) -> str:
        return f"<{DownloadFailedError.__name__} file_name={self.file_name}, reason={self.reason}, inner_exception={type(self.inner_exception)}>"


__all__ = [
    DownloadFailedError.__name__,
]
