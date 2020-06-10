#!/usr/bin/env python3
from pathlib import Path

import structlog

from common.data_filename import DataFilename

log = structlog.get_logger()


class DataFileLinker(object):

    def __init__(self, *, data_path: Path, out_path: Path,
                 source_type_index: int,
                 year_index: int,
                 month_index: int,
                 day_index: int):
        """
        Constructor.

        :param data_path: The path to data files.
        :param out_path: The path to write output.
        :param source_type_index: The source type index in the data path.
        :param year_index: The year index in the data path.
        :param month_index: The month index in the data path.
        :param day_index: The day index in the data path.
        """
        self.data_path = data_path
        self.out_path = out_path
        self.data_source_type_index = source_type_index
        self.data_year_index = year_index
        self.data_month_index = month_index
        self.data_day_index = day_index

    def link_files(self):
        """Link the data files into the output path and yield the source ID and output path for each data file."""
        for path in self.data_path.rglob('*'):
            if path.is_file():
                log.debug(f'data file path: {path}')
                filename = path.name
                parts = path.parts
                source_type = parts[self.data_source_type_index]
                year = parts[self.data_year_index]
                month = parts[self.data_month_index]
                day = parts[self.data_day_index]
                source_id = DataFilename(filename).source_id()
                log.debug(f'type: {source_type} Y: {year} M: {month} D: {day} id: {source_id} file: {filename}')
                output_path = Path(self.out_path, source_type, year, month, day, source_id)
                link_path = Path(output_path, 'data', filename)
                link_path.parent.mkdir(parents=True, exist_ok=True)
                link_path.symlink_to(path)
                yield {'source_id': source_id, 'output_path': output_path}
