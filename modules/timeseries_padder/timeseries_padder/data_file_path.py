#!/usr/bin/env python3
from pathlib import Path
from typing import Tuple


class DataFilePath(object):

    def __init__(self, *, year_index: int, month_index: int, day_index: int, location_index: int,
                 data_type_index: int):
        self.year_index = year_index
        self.month_index = month_index
        self.day_index = day_index
        self.location_index = location_index
        self.data_type_index = data_type_index

    def parse(self, path: Path) -> Tuple[str, str, str, str, str]:
        parts = path.parts
        year: str = parts[self.year_index]
        month: str = parts[self.month_index]
        day: str = parts[self.day_index]
        location: str = parts[self.location_index]
        data_type: str = parts[self.data_type_index]
        return year, month, day, location, data_type
