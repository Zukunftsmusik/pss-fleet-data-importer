from datetime import datetime
from typing import Any, Mapping

from pss_fleet_data import PssFleetDataClient

from src.app.core.gdrive import GDriveFile, GoogleDriveClient


class MockGoogleDriveClient(GoogleDriveClient):
    def __init__(self):
        pass


class MockPssFleetDataClient(PssFleetDataClient):
    def __init__(self):
        pass


class MockGDriveFile(GDriveFile):
    def __init__(self, file_id: str, file_name: str, file_size: int, modified_date: datetime, content: str):
        self.id = file_id
        self.name = file_name
        self.size = file_size
        self.modified_date = modified_date
        self.content = content

    def get_content_string(self, *args: Any, **kwargs: Mapping[str, Any]):
        return self.content
