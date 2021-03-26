#!/usr/bin/env python3
from pathlib import Path
from calendar import monthrange
import structlog

from date_gap_filler.dates_between import date_is_between
from date_gap_filler.date_gap_filler_config import DateGapFillerConfig
from date_gap_filler.location_path_parser import LocationPathParser
import date_gap_filler.empty_files as empty_files

log = structlog.get_logger()


class LocationFileLinker:
    """
    Class to link location files from the input repository into the output
    repository. If the day is missing from a location file path empty placeholder
    files are linked into each day for the month containing the missing day.
    """

    def __init__(self, config: DateGapFillerConfig) -> None:
        self.config = config
        self.out_path = config.out_path
        self.location_path = config.location_path
        self.start_date = config.start_date
        self.end_date = config.end_date
        self.location_dir = config.location_dir
        self.location_path_parser = LocationPathParser(config)

    def link_files(self) -> None:
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
                        day_str = str(day).zfill(2)
                        root_link_path = Path(self.out_path, source_type, year, month, day_str, location)
                        self.link_location(root_link_path, path)
                        empty_files.link_files(self.config, root_link_path, location, year, month, day_str)
                else:
                    if not date_is_between(year=int(year), month=int(month), day=int(day),
                                           start_date=self.start_date, end_date=self.end_date):
                        continue
                    root_link_path = Path(self.out_path, source_type, year, month, day, location)
                    self.link_location(root_link_path, path)
                    empty_files.link_files(self.config, root_link_path, location, year, month, day)

    def link_location(self, root_link_path: Path, path: Path) -> None:
        location_link = Path(root_link_path, self.location_dir, path.name)
        location_link.parent.mkdir(parents=True, exist_ok=True)
        if not location_link.exists():
            location_link.symlink_to(path)
