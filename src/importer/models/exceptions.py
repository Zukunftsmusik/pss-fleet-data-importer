class DownloadFailedError(Exception):
    def __init__(self, file_name: str, reason: str):
        self.file_name = file_name
        self.reason = reason
        self.message = f"Download of file '{file_name}' failed: {reason}"
        super().__init__(self.message)

    def __str__(self) -> str:
        return self.message

    def __repr__(self) -> str:
        return f"<{DownloadFailedError.__name__} file_name={self.file_name}, reason={self.reason}>"


__all__ = [
    DownloadFailedError.__name__,
]
