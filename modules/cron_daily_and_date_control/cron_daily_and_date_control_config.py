from typing import NamedTuple
from pathlib import Path
from datetime import datetime


class Config(NamedTuple):
    site_file_path: Path
    out_path: Path
    source_type: str
    start_date: datetime
    end_date: datetime
