from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class CollectionFileChange:
    downloaded_at: Optional[datetime] = None
    imported_at: Optional[datetime] = None
    download_errors: Optional[int] = None

    def __str__(self) -> str:
        changes = []

        if self.downloaded_at:
            changes.append(f"downloaded_at={self.downloaded_at.isoformat()}")

        if self.imported_at:
            changes.append(f"imported_at={self.imported_at.isoformat()}")

        if self.download_errors:
            changes.append(f"download_errors={self.download_errors}")

        return ", ".join(changes)

    def __repr__(self) -> str:
        attributes = ", ".join(
            [
                f"downloaded_at={self.downloaded_at.isoformat() if self.downloaded_at else None}",
                f"imported_at={self.imported_at.isoformat() if self.imported_at else None}",
                f"download_errors={self.download_errors}",
            ]
        )
        return f"<CollectionFileChange: {attributes}>"


__all__ = [
    CollectionFileChange.__name__,
]
