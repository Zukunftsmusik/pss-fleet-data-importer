from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from ..core.config import CONFIG
from . import callbacks


from_date_default = CONFIG.earliest_data_date.strftime("%Y-%m-%d %H:%M:%S")
temp_dir_default = "./collections/"
out_dir_default = "./"


client_email = Annotated[
    str,
    typer.Option(
        "--client_email",
        help="The email address of the Google Service Account.",
        envvar="GDRIVE_CLIENT_KEY",
    ),
]

client_id = Annotated[
    str,
    typer.Option(
        "--client_id",
        help="The ID of the Google Service Account.",
        envvar="GDRIVE_CLIENT_ID",
    ),
]

from_date = Annotated[
    datetime,
    typer.Option(
        "--from-date",
        "-f",
        callback=callbacks.check_date,
        formats=["%Y-%m-%d", "%Y-%m-%d %H:%M:%S"],
        help="The earliest time from when to import collections.",
    ),
]

gdrive_folder_id = Annotated[
    str,
    typer.Option(
        "--gdrive-folder-id",
        "-g",
        help="The ID of the Google Drive folder to download the Fleet Data from.",
    ),
]

out_dir = Annotated[
    Path,
    typer.Option(
        "--out-dir",
        "-o",
        file_okay=False,
        dir_okay=True,
        writable=True,
        readable=True,
        help="The directory where Collections get downloaded to, before they get imported.",
    ),
]

private_key = Annotated[
    str,
    typer.Option(
        "--private-key",
        help="The private key of the Google Service Account. Alternatively specify the parameter 'private_key_path'.",
        envvar="GDRIVE_SERVICE_PRIVATE_KEY",
    ),
]

private_key_id = Annotated[
    str,
    typer.Option(
        "--private-key-id",
        help="The ID of the private key of the Google Service Account.",
        envvar="GDRIVE_SERVICE_PRIVATE_KEY_ID",
    ),
]

private_key_path = Annotated[
    Path,
    typer.Option(
        "--private_key_path",
        "-p",
        file_okay=True,
        dir_okay=False,
        readable=True,
        help="The path to the file containing the private key for the Google Service Account. If specified overrides parameter 'private_key'.",
    ),
]

project_id = Annotated[
    str,
    typer.Option(
        "--project-id",
        help="The Project ID of the Google Service Account.",
        envvar="GDRIVE_SERVICE_PROJECT_ID",
    ),
]

server_url = Annotated[
    str,
    typer.Option(
        "--url",
        "-u",
        help="The base URL of the PSS Fleet Data API server.",
    ),
]

temp_dir = Annotated[
    Path,
    typer.Option(
        "--temp-dir",
        "-t",
        dir_okay=True,
        writable=True,
        readable=True,
        help="The directory where Collections get downloaded to, before they get imported.",
    ),
]

to_date = Annotated[
    datetime,
    typer.Option(
        "--to-date",
        "-t",
        callback=callbacks.check_date,
        formats=["%Y-%m-%d", "%Y-%m-%d %H:%M:%S"],
        help="The latest time from when to import collections. If not specified, will download all Collections up to now.",
    ),
]

verbose = Annotated[
    bool,
    typer.Option(
        "--verbose",
        "-v",
        is_flag=True,
        flag_value=True,
        help="Print the full progress including the file names of all downloaded and imported Collections.",
    ),
]

watcher = Annotated[
    bool,
    typer.Option(
        "--watcher",
        "-w",
        is_flag=True,
        flag_value=True,
        help="After importing all Collections meeting the criteria, launch a watcher that will automatically download and import any new Collections added to the gdrive.",
    ),
]
