from ..core.config import CONFIG
from . import options


def command(
    from_date: options.from_date = options.from_date_default,
    gdrive_folder_id: options.gdrive_folder_id = CONFIG.default_gdrive_folder_id,
    server_url: options.server_url = CONFIG.default_api_server_url,
    temp_dir: options.temp_dir = options.temp_dir_default,
    verbose: options.verbose = None,
    watcher: options.watcher = None,
):
    pass
