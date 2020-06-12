#!/usr/bin/env python3
from pathlib import Path

import structlog

from common.data_filename import DataFilename

from data_calibration_group.data_file_path import DataFilePath

log = structlog.get_logger()


class DataFileLinker(object):

    def __init__(self, *, data_path: Path, out_path: Path, data_file_path: DataFilePath):
        """
        Constructor.

        :param data_path: The path to data files.
        :param out_path: The path to write output.
        :param data_file_path: The file path parser.
        """
        self.data_path = data_path
        self.out_path = out_path
        self.data_file_path = data_file_path

    def link_files(self):
        """Link the data files into the output path and yield the source ID and output path for each data file."""
        for path in self.data_path.rglob('*'):
            if path.is_file():
                log.debug(f'data file path: {path}')
                source_type, year, month, day = self.data_file_path.parse(path)
                source_id = DataFilename(path.name).source_id()
                log.debug(f'type: {source_type} Y: {year} M: {month} D: {day} id: {source_id} file: {path.name}')
                output_path = Path(self.out_path, source_type, year, month, day, source_id)
                link_path = Path(output_path, 'data', path.name)
                link_path.parent.mkdir(parents=True, exist_ok=True)
                link_path.symlink_to(path)
                yield {'source_id': source_id, 'output_path': output_path}
