import json
import logging
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Generator, Optional

import pydrive2.auth
import pydrive2.drive
import pydrive2.files
import yaml
from pydrive2.files import GoogleDriveFile, GoogleDriveFileList

from . import utils


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
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.__logger = logger.getChild(GoogleDriveClient.__name__) if logger else logging.getLogger(GoogleDriveClient.__name__)

        self.logger.info(f"Creating {GoogleDriveClient.__name__}")

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

        self.__initialize()

    @property
    def logger(self) -> logging.Logger:
        return self.__logger

    def get_file_contents(self, file: pydrive2.files.GoogleDriveFile) -> str:
        file_name = utils.get_gdrive_file_name(file)

        try:
            self.logger.debug("Downloading file: %s", file_name)
            result = file.GetContentString()
            self.logger.debug("Downloaded file: %s", file_name)
        except (pydrive2.files.ApiRequestError, pydrive2.files.FileNotDownloadableError) as exc:
            self.logger.warn("An error occured while downloading file '%s': %s", file_name, exc)
            raise exc

        return result

    def list_all_files(self) -> Generator[pydrive2.files.GoogleDriveFile, None, None]:
        self.__ensure_initialized()

        params = {
            "orderBy": "createdDate",
            "q": self.__base_criteria,
        }
        file_list = self.__drive.ListFile(param=params).GetList()

        for file in file_list:
            yield file

    def list_files_by_modified_date(
        self, modified_after: Optional[datetime] = None, modified_before: Optional[datetime] = None
    ) -> Generator[pydrive2.files.GoogleDriveFile, None, None]:
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

        file_list: list[pydrive2.files.GoogleDriveFile] = self.__drive.ListFile(param=params).GetList()

        for file in file_list:
            yield file

    def __ensure_initialized(self) -> None:
        try:
            self.__drive.ListFile({"q": f"{self.__base_criteria} and title contains 'highaöegjoyödfmj giod'"}).GetList()
        except pydrive2.auth.InvalidConfigError:
            self.__initialize()

    def __initialize(self) -> None:
        service_account_file = Path(self.__service_account_file_path)
        if service_account_file.exists() and not utils.is_empty_file(service_account_file):
            self.logger.info(f"Using existing Service Account Credentials file: {service_account_file.absolute()}")
        else:
            GoogleDriveClient.create_service_account_credential_json(
                self.__project_id,
                self.__private_key_id,
                self.__private_key,
                self.__client_email,
                self.__client_id,
                self.__service_account_file_path,
            )
            self.logger.info(f"Created Service Account Credentials file: {service_account_file.absolute()}")

        settings_file = Path(self.__settings_file_path)
        if settings_file.exists() and not utils.is_empty_file(settings_file):
            self.logger.info(f"Using existing Settings file: {settings_file.absolute()}")
        else:
            GoogleDriveClient.create_service_account_settings_yaml(
                self.__settings_file_path,
                self.__service_account_file_path,
                self.__scopes,
            )
            self.logger.info(f"Created Settings file: {settings_file.absolute()}")

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


__all__ = [
    GoogleDriveClient.__name__,
    # External references
    GoogleDriveFile.__name__,
    GoogleDriveFileList.__name__,
]
