#!/usr/bin/env python3
from pathlib import Path
from typing import Tuple

from location_daily_linker.location_daily_linker_config import Config


class LocationPathParser:

    def __init__(self, config: Config):
        self.source_type_index = config.source_type_index
        self.year_index = config.year_index
        self.month_index = config.month_index
        self.location_index = config.location_index

    def parse(self, path: Path) -> Tuple[str, str, str, str]:
        parts = path.parts
        source_type: str = parts[self.source_type_index]
        year: str = parts[self.year_index]
        month: str = parts[self.month_index]
        location: str = parts[self.location_index]
        return source_type, year, month, location
