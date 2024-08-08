from datetime import datetime
from typing import Optional

from sqlmodel import Field, SQLModel


class CollectionFileBase(SQLModel):
    collection_file_id: int = Field(primary_key=True, index=True, default=None, sa_column_kwargs={"name": "id"})
    gdrive_file_id: str = Field(index=True, unique=True)
    file_name: str = Field(index=True, unique=True)
    gdrive_modified_date: datetime = Field(index=True)
    timestamp: datetime = Field(index=True, unique=True)
    downloaded_at: Optional[datetime] = Field(default=None, nullable=True)
    imported_at: Optional[datetime] = Field(default=None, nullable=True)
    download_error: Optional[bool] = Field(default=None, nullable=True)


class CollectionFileDB(CollectionFileBase, table=True):
    __tablename__ = "collection_file"


__all__ = [
    CollectionFileBase.__name__,
    CollectionFileDB.__name__,
]
