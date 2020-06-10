#!/usr/bin/env python3
from pathlib import Path
import structlog

log = structlog.get_logger()


class EventLocationGrouper(object):

    def __init__(self, *,
                 data_path: Path,
                 location_path: Path,
                 out_path: Path,
                 source_type_index: int,
                 year_index: int,
                 month_index: int,
                 day_index: int,
                 source_id_index: int,
                 filename_index: int):
        """
        Constructor.

        :param data_path: The path to the data files.
        :param location_path: The path to the location file.
        :param out_path: The path for writing results.
        :param source_type_index: The input file path index of the source type.
        :param year_index: The input file path index of the year.
        :param month_index: The input file path index of the month.
        :param day_index: The input file path index of the day.
        :param source_id_index: The input file path index of the source ID.
        :param filename_index: The input file path index of the filename.
        """
        self.data_path = data_path
        self.location_path = location_path
        self.out_path = out_path
        self.source_type_index = source_type_index
        self.year_index = year_index
        self.month_index = month_index
        self.day_index = day_index
        self.source_id_index = source_id_index
        self.filename_index = filename_index

    def group(self):
        """Link event data and location files into output path."""
        for path in self.data_path.rglob('*'):
            if path.is_file():
                parts = path.parts
                source_type = parts[self.source_type_index]
                year = parts[self.year_index]
                month = parts[self.month_index]
                day = parts[self.day_index]
                source_id = parts[self.source_id_index]
                filename = parts[self.filename_index]
                log.debug(f'filename: {filename} source type: {source_type} source_id: {source_id}')
                link_root_path = Path(self.out_path, source_type, year, month, day, source_id)
                self.link_location(link_root_path)
                link_path = Path(link_root_path, 'data', filename)
                log.debug(f'data link: {link_path}')
                link_path.parent.mkdir(parents=True, exist_ok=True)
                link_path.symlink_to(path)

    def link_location(self, link_root_path: Path):
        """
        Link the location file into the target directory.

        :param link_root_path: The target directory path.
        """
        for path in self.location_path.rglob('*'):
            if path.is_file():
                link_path = Path(link_root_path, 'location', path.name)
                log.debug(f'location link: {link_path}')
                link_path.parent.mkdir(parents=True, exist_ok=True)
                link_path.symlink_to(path)
