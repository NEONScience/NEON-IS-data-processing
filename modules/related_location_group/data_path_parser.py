#!/usr/bin/env python3
from pathlib import Path
from typing import Tuple

from related_location_group.related_location_group_config import Config


class DataPathParser:

    def __init__(self, config: Config) -> None:
        self.source_type_index = config.source_type_index
        self.year_index = config.year_index
        self.month_index = config.month_index
        self.day_index = config.day_index
        self.group_index = config.group_index
        self.location_index = config.location_index
        self.data_type_index = config.data_type_index
        self.max_value = max([self.source_type_index, self.year_index, self.month_index, self.day_index,
                              self.location_index, self.data_type_index])

    def parse_file(self, path: Path) -> Tuple[str, str, str, str, str, str, str, Tuple[str]]:
        parts = path.parts
        source_type: str = parts[self.source_type_index]
        year: str = parts[self.year_index]
        month: str = parts[self.month_index]
        day: str = parts[self.day_index]
        group: str = parts[self.group_index]
        location: str = parts[self.location_index]
        data_type: str = parts[self.data_type_index]
        remainder: Tuple[str] = parts[self.max_value + 1:]
        return source_type, year, month, day, group, location, data_type, remainder

    def parse_dir(self, path: Path) -> Tuple[str, str, str, str, str, str, str]:
        parts = path.parts
        source_type: str = parts[self.source_type_index]
        year: str = parts[self.year_index]
        month: str = parts[self.month_index]
        day: str = parts[self.day_index]
        group: str = parts[self.group_index]
        location: str = parts[self.location_index]
        data_type: str = parts[self.data_type_index]
        return source_type, year, month, day, group, location, data_type
