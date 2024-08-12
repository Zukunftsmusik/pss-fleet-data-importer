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


__all__ = [
    ImporterBaseError.__name__,
]
