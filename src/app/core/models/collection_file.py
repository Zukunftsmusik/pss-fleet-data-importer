from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class CollectionFileBase(BaseModel):
    collection_file_id: int
    gdrive_file_id: str
    file_name: str
    gdrive_modified_date: datetime
    timestamp: datetime
    downloaded_at: Optional[datetime]
    imported_at: Optional[datetime]
    download_error: Optional[bool]
