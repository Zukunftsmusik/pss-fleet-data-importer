from datetime import datetime

import pytest
from pydrive2.files import GoogleDriveFile

from src.app.core.gdrive import GDriveFile


def test_create(
    google_drive_file: GoogleDriveFile,
    google_drive_file_id: str,
    google_drive_file_size: int,
    google_drive_file_name: str,
    google_drive_file_modified_date: datetime,
):
    gdrive_file = GDriveFile(google_drive_file)
    assert gdrive_file.id == google_drive_file_id
    assert gdrive_file.size == google_drive_file_size
    assert gdrive_file.name == google_drive_file_name
    assert gdrive_file.modified_date == google_drive_file_modified_date


def test_get_content_string_returns_content(gdrive_file: GDriveFile, google_drive_file_content: str, monkeypatch: pytest.MonkeyPatch):
    def mock_GetContentString(*args):
        return google_drive_file_content

    monkeypatch.setattr(GoogleDriveFile, GoogleDriveFile.GetContentString.__name__, mock_GetContentString)

    assert gdrive_file.get_content_string() == google_drive_file_content
