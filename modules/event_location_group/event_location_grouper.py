#!/usr/bin/env python3
from pathlib import Path
import structlog

from event_location_group.data_file_path import DataFilePath

log = structlog.get_logger()


class EventLocationGrouper(object):

    def __init__(self, *, data_path: Path, location_path: Path, out_path: Path, data_file_path: DataFilePath):
        """
        Constructor.

        :param data_path: The path to the data files.
        :param location_path: The path to the location file.
        :param out_path: The path for writing results.
        :param data_file_path: The file path parser.
        """
        self.data_path = data_path
        self.location_path = location_path
        self.out_path = out_path
        self.data_file_path = data_file_path

    def group_files(self):
        """Link event data and location files into output path."""
        for path in self.data_path.rglob('*'):
            if path.is_file():
                source_type, year, month, day, source_id = self.data_file_path.parse(path)
                log.debug(f'file: {path.name} source_type: {source_type} source_id: {source_id}')
                link_root_path = Path(self.out_path, source_type, year, month, day, source_id)
                self.link_location(link_root_path)
                link_path = Path(link_root_path, 'data', path.name)
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
