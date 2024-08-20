from datetime import datetime

from pydantic import BaseModel


class CollectionFileBase(BaseModel):
    collection_file_id: int
    gdrive_file_id: str
    file_name: str
    gdrive_modified_date: datetime
    timestamp: datetime
    imported: bool
    error: bool
