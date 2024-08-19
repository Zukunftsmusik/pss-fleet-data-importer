from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class CollectionFileChange:
    collection_file_id: int = None
    imported: Optional[bool] = None
    error: Optional[bool] = None

    def __str__(self) -> str:
        changes = []

        if self.imported is not None:
            changes.append(f"imported={self.imported}")

        if self.error is not None:
            changes.append(f"error={self.error}")

        return ", ".join(changes)

    def __repr__(self) -> str:
        attributes = ", ".join(
            [
                f"collection_file_id={self.collection_file_id}",
                f"imported={self.imported}",
                f"error={self.error}",
            ]
        )
        return f"<CollectionFileChange: {attributes}>"


__all__ = [
    CollectionFileChange.__name__,
]
