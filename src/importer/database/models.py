from datetime import datetime
from typing import Optional

from sqlmodel import Field, SQLModel


class CollectionFileBase(SQLModel):
    id: int = Field(primary_key=True, index=True, default=None)
    file_name: str = Field(index=True)
    timestamp: datetime = Field(index=True)
    downloaded_at: Optional[datetime] = Field(default=None, nullable=True)
    imported_at: Optional[datetime] = Field(default=None, nullable=True)


class CollectionFileDB(CollectionFileBase, table=True):
    __tablename__ = "collection_file"


__all__ = [
    CollectionFileBase.__name__,
    CollectionFileDB.__name__,
]
