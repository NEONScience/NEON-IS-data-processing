#!/usr/bin/env python3
from pathlib import Path

from structlog import get_logger

from related_location_group.data_path_parser import DataPathParser
from related_location_group.related_location_group_config import Config

log = get_logger()


class RelatedLocationGrouper:

    def __init__(self, config: Config) -> None:
        self.data_path = config.data_path
        self.out_path = config.out_path
        self.data_path_parser = DataPathParser(config)

    def group_files(self) -> None:
        """Link related data and location files into the output path."""
        for path in self.data_path.rglob('*'):
            # create data type directories as they may be empty
            if path.is_dir() and len(path.parts) - 1 == self.data_path_parser.max_value:
                source_type, year, month, day, group, location, data_type = self.data_path_parser.parse_dir(path)
                output_path = Path(self.out_path, year, month, day, group, source_type, location, data_type)
                output_path.mkdir(parents=True, exist_ok=True)
            elif path.is_file():
                source_type, year, month, day, group, location, data_type, remainder = \
                    self.data_path_parser.parse_file(path)
                link_path = Path(self.out_path, year, month, day, group, source_type, location, data_type, *remainder)
                link_path.parent.mkdir(parents=True, exist_ok=True)
                if not link_path.exists():
                    link_path.symlink_to(path)
