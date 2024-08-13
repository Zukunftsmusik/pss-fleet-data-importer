import json
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Generator, Iterable, Optional

import dateutil.parser
import pydrive2.auth
import pydrive2.drive
import yaml
from pydrive2.files import ApiRequestError, FileNotDownloadableError, GoogleDriveFile

from ..log.log_core import gdrive as log
from . import utils


class GDriveFile:
    def __init__(self, google_drive_file: GoogleDriveFile):
        self.id: str = google_drive_file["id"]
        self.name: str = get_gdrive_file_name(google_drive_file)
        self.size: int = int(google_drive_file["fileSize"])
        self.modified_date: datetime = dateutil.parser.parse(google_drive_file["modifiedDate"])
        self.__google_drive_file: GoogleDriveFile = google_drive_file

    def get_content_string(self, mimetype: Optional[str] = None, encoding: str = "utf-8", remove_bom: bool = False):
        return self.__google_drive_file.GetContentString(mimetype, encoding, remove_bom)


class GoogleDriveClient:
    def __init__(
        self,
        project_id: str,
        private_key_id: str,
        private_key: str,
        client_email: str,
        client_id: str,
        scopes: list[str],
        folder_id: str,
        service_account_file_path: str,
        settings_file_path: str,
    ) -> None:
        log.client_creating()

        self.__client_email: str = client_email
        self.__client_id: str = client_id
        self.__folder_id: str = folder_id
        self.__private_key: str = private_key
        self.__private_key_id: str = private_key_id
        self.__project_id: str = project_id
        self.__scopes: tuple[str] = tuple(scopes)
        self.__service_account_file_path: str = service_account_file_path
        self.__settings_file_path: str = settings_file_path

        self.__base_criteria: str = f"'{self.__folder_id}' in parents and title contains 'pss-top-100' and not title contains 'of'"

        self.__gauth: pydrive2.auth.GoogleAuth = None
        self.__drive: pydrive2.drive.GoogleDrive = None

    def get_file_contents(self, file: GDriveFile) -> str:
        try:
            with log.download_file(file.name):
                result = file.get_content_string()
        except (ApiRequestError, FileNotDownloadableError) as exc:
            log.download_file_error(file.name, exc)
            raise exc

        return result

    def list_files_by_modified_date(
        self, modified_after: Optional[datetime] = None, modified_before: Optional[datetime] = None
    ) -> Generator[GDriveFile, None, None]:
        self.__ensure_initialized()

        criteria = [self.__base_criteria]

        if modified_after:
            criteria.append(f"modifiedDate > '{modified_after.isoformat()}'")

        if modified_before:
            criteria.append(f"modifiedDate < '{modified_before.isoformat()}'")

        params = {
            "orderBy": "createdDate",
            "q": " and ".join(criteria),
        }

        google_drive_files: list[GoogleDriveFile] = self.__drive.ListFile(param=params).GetList()
        file_list = FromGoogleDriveFile.to_gdrive_files(google_drive_files)

        for file in file_list:
            yield file

    def __ensure_initialized(self) -> None:
        try:
            self.__drive.ListFile({"q": f"{self.__base_criteria} and title contains 'highaöegjoyödfmj giod'"}).GetList()
        except (pydrive2.auth.InvalidConfigError, AttributeError):
            self.initialize()

    def initialize(self) -> None:
        service_account_file = Path(self.__service_account_file_path)
        if service_account_file.exists() and not utils.is_empty_file(service_account_file):
            log.credentials_json_exists(service_account_file)
        else:
            GoogleDriveClient.create_service_account_credential_json(
                self.__project_id,
                self.__private_key_id,
                self.__private_key,
                self.__client_email,
                self.__client_id,
                self.__service_account_file_path,
            )
            log.credentials_json_created(service_account_file)

        settings_file = Path(self.__settings_file_path)
        if settings_file.exists() and not utils.is_empty_file(settings_file):
            log.settings_yaml_exists(settings_file)
        else:
            GoogleDriveClient.create_service_account_settings_yaml(
                self.__settings_file_path,
                self.__service_account_file_path,
                self.__scopes,
            )
            log.settings_yaml_created(settings_file)

        self.__gauth = pydrive2.auth.GoogleAuth(settings_file=settings_file.absolute())
        credentials = pydrive2.auth.ServiceAccountCredentials.from_json_keyfile_name(
            self.__service_account_file_path,
            self.__scopes,
        )
        self.__gauth.credentials = credentials
        self.__drive = pydrive2.drive.GoogleDrive(self.__gauth)

    @staticmethod
    def create_service_account_credential_json(
        project_id: str,
        private_key_id: str,
        private_key: str,
        client_email: str,
        client_id: str,
        service_account_file_path: str,
    ) -> None:
        contents = {
            "type": "service_account",
            "project_id": project_id,
            "private_key_id": private_key_id,
            "private_key": private_key,
            "client_email": client_email,
            "client_id": client_id,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{urllib.parse.quote(client_email)}",
        }
        with open(service_account_file_path, "w") as fp:
            json.dump(contents, fp, indent=2)

    @staticmethod
    def create_service_account_settings_yaml(
        settings_file_path: str,
        service_account_file_path: str,
        scopes: list[str],
    ) -> None:
        contents = {
            "client_config_backend": "file",
            "client_config_file": service_account_file_path,
            "save_credentials": True,
            "save_credentials_backend": "file",
            "save_credentials_file": "credentials.json",
            "oauth_scope": scopes,
        }

        with open(settings_file_path, "w+") as fp:
            yaml.dump(contents, fp)


class FromGoogleDriveFile:
    @staticmethod
    def to_gdrive_file(source: GoogleDriveFile) -> GDriveFile:
        return GDriveFile(source)

    @staticmethod
    def to_gdrive_files(sources: Iterable[GoogleDriveFile]) -> GDriveFile:
        return [FromGoogleDriveFile.to_gdrive_file(source) for source in sources]


def get_gdrive_file_name(gdrive_file: GoogleDriveFile) -> str:
    """Returns the file name of a `GoogleDriveFile` of API version 2 or 3.

    Args:
        gdrive_file (GoogleDriveFile): The file to retrieve the file name from.

    Returns:
        str: The file name.
    """
    file_name = gdrive_file.get("title") or gdrive_file.get("name")  # "name" is gdrive API V3, "title" is V2
    return file_name


__all__ = [
    # Classes
    GoogleDriveClient.__name__,
    # External references
    GDriveFile.__name__,
]
