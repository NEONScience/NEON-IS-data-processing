#!/usr/bin/env python3
from pathlib import Path
from calendar import monthrange
import structlog

from date_gap_filler.dates_between import date_is_between
from date_gap_filler.empty_file_paths import EmptyFilePaths
from date_gap_filler.empty_file_linker import EmptyFileLinker
from date_gap_filler.date_gap_filler_config import DateGapFillerConfig
from date_gap_filler.location_path_parser import LocationPathParser
from date_gap_filler.empty_file_handler import EmptyFileHandler

log = structlog.get_logger()


class LocationFileLinker:

    def __init__(self, config: DateGapFillerConfig):
        self.out_path = config.out_path
        self.output_dirs = config.output_directories
        self.location_path = config.location_path
        self.start_date = config.start_date
        self.end_date = config.end_date
        self.location_dir = config.location_dir
        self.empty_file_paths = EmptyFilePaths(config)
        self.location_path_parser = LocationPathParser(config)
        self.empty_file_handler = EmptyFileHandler(config)

    def link_files(self):
        """Process the location files and fill date gaps with empty files."""
        for path in self.location_path.rglob('*'):
            if path.is_file():
                log.debug(f'processing location file: {path}')
                source_type, year, month, day, location = self.location_path_parser.parse(path)
                if day is None:
                    days = monthrange(int(year), int(month))[1]
                    for day in range(1, days + 1):
                        if not date_is_between(year=int(year), month=int(month), day=int(day),
                                               start_date=self.start_date, end_date=self.end_date):
                            continue
                        day_string = str(day).zfill(2)
                        self.link_path(path, source_type, year, month, day_string, location)
                else:
                    if not date_is_between(year=int(year), month=int(month), day=int(day),
                                           start_date=self.start_date, end_date=self.end_date):
                        continue
                    self.link_path(path, source_type, year, month, day, location)

    def link_path(self, path: Path, source_type: str, year: str, month: str, day: str, location: str):
        root_link_path = Path(self.out_path, source_type, year, month, day, location)
        location_link = Path(root_link_path, self.location_dir, path.name)
        location_link.parent.mkdir(parents=True, exist_ok=True)
        if not location_link.exists():
            location_link.symlink_to(path)
        empty_file_linker = EmptyFileLinker(self.empty_file_paths, location, year, month, day)
        self.empty_file_handler.link_files(root_link_path, empty_file_linker)
