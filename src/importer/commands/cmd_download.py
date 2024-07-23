from ..core.config import CONFIG
from . import options


def command(
    from_date: options.from_date = options.from_date_default,
    gdrive_folder_id: options.gdrive_folder_id = CONFIG.default_gdrive_folder_id,
    out_dir: options.out_dir = options.out_dir_default,
    to_date: options.to_date = None,
    verbose: options.verbose = None,
):
    pass
