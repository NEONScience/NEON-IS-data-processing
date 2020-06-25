#!/usr/bin/env python3
from pathlib import Path
import structlog

from calibrated_location_group.calibrated_file_path import CalibratedFilePath

log = structlog.get_logger()


class CalibratedLocationFileGrouper(object):
    """Class to group calibrated data files and associated location files."""

    def __init__(self, *, calibrated_path: Path, location_path: Path, out_path: Path,
                 calibrated_file_path: CalibratedFilePath):
        """
        Constructor.

        :param calibrated_path: Path to the calibration files to process.
        :param location_path: Path to the calibration files to process.
        :param out_path: Path to link output.
        :param calibrated_file_path: The calibrated file path parser.
        """
        self.calibrated_path = calibrated_path
        self.location_path = location_path
        self.out_path = out_path
        self.calibrated_file_path = calibrated_file_path

    def group_files(self):
        """
        Link calibrated data and location files into the common output path.
        Files are joined on input and are assumed to represent data from a single source.
        """
        for common_link_path in self.link_calibrated_files():
            self.link_location_files(common_link_path)

    def link_calibrated_files(self):
        """Link calibrated data files into the output path."""
        for path in self.calibrated_path.rglob('*'):
            if path.is_file():
                source_type, year, month, day, source_id, data_type = self.calibrated_file_path.parse(path)
                parts = path.parts
                log.debug(f'year: {year} month: {month} day: {day} source type: {source_type} '
                          f'source_id: {source_id} data type: {data_type}')
                common_link_path = Path(self.out_path, source_type, year, month, day, source_id)
                link_path = Path(common_link_path, data_type, *parts[self.calibrated_file_path.data_type_index + 1:])
                link_path.parent.mkdir(parents=True, exist_ok=True)
                link_path.symlink_to(path)
                yield common_link_path

    def link_location_files(self, common_link_path: Path):
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
