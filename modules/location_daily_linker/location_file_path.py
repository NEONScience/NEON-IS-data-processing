#!/usr/bin/env python3
from pathlib import Path


class LocationFilePath(object):

    def __init__(self, *, source_type_index: int, year_index: int, month_index: int, location_index: int):
        self.source_type_index = source_type_index
        self.year_index = year_index
        self.month_index = month_index
        self.location_index = location_index

    def parse(self, path: Path):
        parts = path.parts
        source_type = parts[self.source_type_index]
        year = parts[self.year_index]
        month = parts[self.month_index]
        location = parts[self.location_index]
        return source_type, year, month, location
