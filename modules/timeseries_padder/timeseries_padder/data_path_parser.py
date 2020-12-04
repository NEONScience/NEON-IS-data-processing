#!/usr/bin/env python3
from pathlib import Path
from typing import Tuple

from timeseries_padder.timeseries_padder.timeseries_padder_config import Config


class DataPathParser:

    def __init__(self, config: Config) -> None:
        self.year_index = config.year_index
        self.month_index = config.month_index
        self.day_index = config.day_index
        self.location_index = config.location_index
        self.data_type_index = config.data_type_index

    def parse(self, path: Path) -> Tuple[str, str, str, str, str]:
        parts = path.parts
        year: str = parts[self.year_index]
        month: str = parts[self.month_index]
        day: str = parts[self.day_index]
        location: str = parts[self.location_index]
        data_type: str = parts[self.data_type_index]
        return year, month, day, location, data_type
