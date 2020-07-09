#!/usr/bin/env python3
import structlog
from pathlib import Path

from date_gap_filler.dates_between import date_is_between
from date_gap_filler.date_gap_filler_config import DateGapFillerConfig
from date_gap_filler.data_path_parser import DataPathParser

log = structlog.get_logger()


class DataFileLinker:

    def __init__(self, config: DateGapFillerConfig):
        self.data_path = config.data_path
        self.out_path = config.out_path
        self.start_date = config.start_date
        self.end_date = config.end_date
        self.data_path_parser = DataPathParser(config)

    def link_files(self):
        """Link all files between the start and end dates."""
        for path in self.data_path.rglob('*'):
            if path.is_file():
                source_type, year, month, day, location, data_type = self.data_path_parser.parse(path)
                if not date_is_between(year=int(year), month=int(month), day=int(day),
                                       start_date=self.start_date, end_date=self.end_date):
                    continue
                link_path = Path(self.out_path, source_type, year, month, day, location, data_type, path.name)
                link_path.parent.mkdir(parents=True, exist_ok=True)
                link_path.symlink_to(path)
