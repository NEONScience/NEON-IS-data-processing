#!/usr/bin/env python3
from pathlib import Path

import structlog

from lib.data_filename import DataFilename
from lib.file_crawler import crawl

log = structlog.get_logger()


class DataGrouper(object):

    def __init__(self,
                 data_path: Path,
                 out_path: Path,
                 data_source_type_index: int,
                 data_year_index: int,
                 data_month_index: int,
                 data_day_index: int):
        """
        Constructor.
        :param data_path: The path to data files.
        :param out_path: The path to write output.
        :param data_source_type_index: The source type index in the data path.
        :param data_year_index: The year index in the data path.
        :param data_month_index: The month index in the data path.
        :param data_day_index: The day index in the data path.
        """
        self.data_path = data_path
        self.out_path = out_path
        self.data_source_type_index = data_source_type_index
        self.data_year_index = data_year_index
        self.data_month_index = data_month_index
        self.data_day_index = data_day_index

    def link_data(self):
        """
        Link the data files into the output directory and yield the source ID and output path
        used for each data file.
        :return:
        """
        for path in crawl(self.data_path):
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
