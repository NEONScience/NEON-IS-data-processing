#!/usr/bin/env python3
from pathlib import Path
import structlog

log = structlog.get_logger()


class CalibratedLocationFileGrouper(object):
    """Class to group_files calibrated data files and associated location files."""

    def __init__(self, *, calibrated_path: Path, location_path: Path, out_path: Path,
                 source_type_index: int,
                 year_index: int,
                 month_index: int,
                 day_index: int,
                 source_id_index: int,
                 data_type_index: int):
        """
        Constructor.

        :param calibrated_path: Path to the calibration files to process.
        :param location_path: Path to the calibration files to process.
        :param out_path: Path to link output.
        :param source_type_index: The source type index in the calibrated file path.
        :param year_index: The year index in the calibrated file path.
        :param month_index: The month index in the calibrated file path.
        :param day_index: The day index in the calibrated file path.
        :param source_id_index: The source ID index in the calibrated file path.
        :param data_type_index: The data type index in the calibrated file path.
        """
        self.calibrated_path = calibrated_path
        self.location_path = location_path
        self.out_path = out_path
        self.source_type_index = source_type_index
        self.year_index = year_index
        self.month_index = month_index
        self.day_index = day_index
        self.source_id_index = source_id_index
        self.data_type_index = data_type_index

    def group(self):
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
                parts = path.parts
                source_type = parts[self.source_type_index]
                year = parts[self.year_index]
                month = parts[self.month_index]
                day = parts[self.day_index]
                source_id = parts[self.source_id_index]
                data_type = parts[self.data_type_index]
                log.debug(f'year: {year} month: {month} day: {day} source type: {source_type} '
                          f'source_id: {source_id} data type: {data_type}')
                common_link_path = Path(self.out_path, source_type, year, month, day, source_id)
                link_path = Path(common_link_path, data_type, *parts[self.data_type_index + 1:])
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
