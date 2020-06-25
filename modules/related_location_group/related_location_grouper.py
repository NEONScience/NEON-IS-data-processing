#!/usr/bin/env python3
from pathlib import Path

from structlog import get_logger

from related_location_group.data_file_path import DataFilePath

log = get_logger()


class RelatedLocationGrouper(object):

    def __init__(self, *, data_path: Path, out_path: Path, data_file_path: DataFilePath):
        """
        Link related data and location files into the output path.

        :param data_path: Path for reading files.
        :param out_path: The output path for linking files into.
        :param data_file_path: The file path parser.
        """
        self.data_path = data_path
        self.out_path = out_path
        self.data_file_path = data_file_path

    def group_files(self):
        """Link related data and location files into the output path."""
        for path in self.data_path.rglob('*'):
            if path.is_file():
                source_type, year, month, day, group, location, data_type, remainder = self.data_file_path.parse(path)
                link_path = Path(self.out_path, year, month, day, group, source_type, location, data_type, *remainder)
                link_path.parent.mkdir(parents=True, exist_ok=True)
                if not link_path.exists():
                    link_path.symlink_to(path)
