from datetime import datetime

import typer

from ..core.config import CONFIG


def check_date(dt: datetime) -> datetime:
    if dt < CONFIG.earliest_data_date:
        raise typer.BadParameter("There is no data before that date.")
    return dt
