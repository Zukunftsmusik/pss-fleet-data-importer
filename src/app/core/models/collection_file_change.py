from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class CollectionFileChange:
    collection_file_id: int = None
    downloaded_at: Optional[datetime] = None
    imported_at: Optional[datetime] = None
    error: Optional[bool] = None

    def __str__(self) -> str:
        changes = []

        if self.downloaded_at:
            changes.append(f"downloaded_at={self.downloaded_at.isoformat()}")

        if self.imported_at:
            changes.append(f"imported_at={self.imported_at.isoformat()}")

        if self.error is not None:
            changes.append(f"error={self.error}")

        return ", ".join(changes)

    def __repr__(self) -> str:
        attributes = ", ".join(
            [
                f"collection_file_id={self.collection_file_id}",
                f"downloaded_at={self.downloaded_at.isoformat() if self.downloaded_at else None}",
                f"imported_at={self.imported_at.isoformat() if self.imported_at else None}",
                f"error={self.error}",
            ]
        )
        return f"<CollectionFileChange: {attributes}>"


__all__ = [
    CollectionFileChange.__name__,
]
