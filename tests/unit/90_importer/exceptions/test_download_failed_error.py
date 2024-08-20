from src.app.importer.exceptions import DownloadFailedError


def test_create(google_drive_file_name: str):
    reason = "permission denied"
    inner = Exception()
    download_failed_error = DownloadFailedError(google_drive_file_name, reason, inner)

    assert download_failed_error.file_name == google_drive_file_name
    assert download_failed_error.reason == reason
    assert download_failed_error.inner_exception is inner
    assert google_drive_file_name in download_failed_error.message
    assert reason in download_failed_error.message
