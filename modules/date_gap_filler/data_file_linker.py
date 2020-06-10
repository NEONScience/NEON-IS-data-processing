#!/usr/bin/env python3
import structlog
from pathlib import Path

from date_gap_filler.date_between import date_is_between
from date_gap_filler.app_config import AppConfig

log = structlog.get_logger()


class DataFileLinker(object):

    def __init__(self, config: AppConfig):
        self.data_path = config.data_path
        self.out_path = config.out_path
        self.source_type_index = config.data_source_type_index
        self.year_index = config.data_year_index
        self.month_index = config.data_month_index
        self.day_index = config.data_day_index
        self.location_index = config.data_location_index
        self.data_type_index = config.data_type_index
        self.filename_index = config.data_filename_index
        self.start_date = config.start_date
        self.end_date = config.end_date

    def link_files(self):
        """Link all files between the start and end dates."""
        for path in self.data_path.rglob('*'):
            if path.is_file():
                parts = path.parts
                source_type = parts[self.source_type_index]
                year = parts[self.year_index]
                month = parts[self.month_index]
                day = parts[self.day_index]
                location = parts[self.location_index]
                data_type = parts[self.data_type_index]
                filename = parts[self.filename_index]
                if not date_is_between(year=int(year), month=int(month), day=int(day),
                                       start_date=self.start_date, end_date=self.end_date):
                    continue
                link_path = Path(self.out_path, source_type, year, month, day, location, data_type, filename)
                link_path.parent.mkdir(parents=True, exist_ok=True)
                link_path.symlink_to(path)
