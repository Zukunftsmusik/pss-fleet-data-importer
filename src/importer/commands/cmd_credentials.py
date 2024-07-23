import json
import os
import sys
import urllib

from . import options


def command(
    client_email: options.client_email,
    client_id: options.client_id,
    private_key_id: options.private_key_id,
    private_key: options.private_key,
    project_id: options.project_id,
    out_dir: options.out_dir = options.out_dir_default,
    private_key_path: options.private_key_path = None,
):
    if private_key_path:
        sys
        with open(private_key_path, "r") as fp:
            private_key = fp.read()

    create_service_account_credential_json(project_id, private_key_id, private_key, client_email, client_id, out_dir.absolute())


def create_service_account_credential_json(
    project_id: str,
    private_key_id: str,
    private_key: str,
    client_email: str,
    client_id: str,
    service_account_path: str,
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

    file_path = os.path.join(service_account_path, "credentials.json")
    with open(file_path, "w") as service_file:
        json.dump(contents, service_file, indent=2)

    print(f"Created service account connection file at: {file_path}")
