#!/usr/bin/env python3
from pathlib import Path
from calendar import monthrange

import structlog

from location_daily_linker.location_path_parser import LocationPathParser
from location_daily_linker.location_daily_linker_config import Config

log = structlog.get_logger()


class LocationDailyLinker:

    def __init__(self, config: Config):
        self.location_path = config.location_path
        self.out_path = config.out_path
        self.location_path_parser = LocationPathParser(config)

    def link_files(self):
        """Link a location file for each date with path '/source/yyyy/mm/dd/location/file'."""
        for path in self.location_path.rglob('*'):
            if path.is_file():
                source_type, year, month, location = self.location_path_parser.parse(path)
                days = monthrange(int(year), int(month))[1]
                for day in range(1, days + 1):
                    day_string = str(day).zfill(2)
                    link_path = Path(self.out_path, source_type, year, month, day_string, location, path.name)
                    log.debug(f'link: {link_path}')
                    link_path.parent.mkdir(parents=True, exist_ok=True)
                    if not link_path.exists():
                        link_path.symlink_to(path)
