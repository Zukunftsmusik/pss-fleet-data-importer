from datetime import datetime
from typing import Optional

from sqlmodel import Field, SQLModel

from ..core.models import CollectionFileBase


class CollectionFileDB(SQLModel, CollectionFileBase, table=True):
    __tablename__ = "collection_file"

    collection_file_id: int = Field(primary_key=True, index=True, default=None, sa_column_kwargs={"name": "id"})
    gdrive_file_id: str = Field(index=True, unique=True)
    file_name: str = Field(index=True, unique=True)
    gdrive_modified_date: datetime = Field(index=True)
    timestamp: datetime = Field(index=True, unique=True)
    downloaded_at: Optional[datetime] = Field(default=None, nullable=True)
    imported_at: Optional[datetime] = Field(default=None, nullable=True)
    error: Optional[bool] = Field(default=None, nullable=True)


__all__ = [
    CollectionFileDB.__name__,
]
