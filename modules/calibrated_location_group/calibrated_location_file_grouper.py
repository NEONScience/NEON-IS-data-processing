#!/usr/bin/env python3
from pathlib import Path
import structlog
from typing import Iterator

from calibrated_location_group.calibrated_location_group_config import Config
from calibrated_location_group.calibrated_path_parser import CalibratedPathParser

log = structlog.get_logger()


class CalibratedLocationFileGrouper:
    """Class to group calibrated data files and associated location files."""

    def __init__(self, config: Config) -> None:
        self.calibrated_path = config.calibrated_path
        self.location_path = config.location_path
        self.out_path = config.out_path
        self.path_parser = CalibratedPathParser(config)

    def group_files(self) -> None:
        """
        Link calibrated data and location files into the common output path.
        Files are joined on input and are assumed to represent data from a single source.
        """
        for common_link_path in self.link_calibrated_files():
            self.link_location_files(common_link_path)

    def link_calibrated_files(self) -> Iterator[Path]:
        """Link calibrated data files into the output path."""
        for path in self.calibrated_path.rglob('*'):
            if path.is_file():
                source_type, year, month, day, source_id, data_type, remainder = self.path_parser.parse(path)
                log.debug(f'year: {year} month: {month} day: {day} source type: {source_type} '
                          f'source_id: {source_id} data type: {data_type}')
                common_link_path = Path(self.out_path, source_type, year, month, day, source_id)
                link_path = Path(common_link_path, data_type, *remainder)
                link_path.parent.mkdir(parents=True, exist_ok=True)
                if not link_path.exists():
                    link_path.symlink_to(path)
                yield common_link_path

    def link_location_files(self, common_link_path: Path) -> None:
        """
        Link location files into the common link path.

        :param common_link_path: The common path for links.
        """
        for path in self.location_path.rglob('*'):
            if path.is_file():
                link_path = Path(common_link_path, 'location', path.name)
                link_path.parent.mkdir(parents=True, exist_ok=True)
                if not link_path.exists():
                    link_path.symlink_to(path)
