#!/usr/bin/env python3
import pathlib

import structlog

from lib.file_linker import link
from lib.file_crawler import crawl
from lib.data_filename import DataFilename

log = structlog.get_logger()


class DataGrouper(object):

    def __init__(self,
                 data_path,
                 out_path,
                 data_source_type_index,
                 data_year_index,
                 data_month_index,
                 data_day_index):
        """
        Constructor.
        :param data_path: The path to data files.
        :type data_path: str
        :param out_path: The path to write output.
        :type out_path: str
        :param data_source_type_index: The source type index in the data path.
        :type data_source_type_index: int
        :param data_year_index: The year index in the data path.
        :type data_year_index: int
        :param data_month_index: The month index in the data path.
        :type data_month_index: int
        :param data_day_index: The day index in the data path.
        :type data_day_index: int
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
        for file_path in crawl(self.data_path):
            log.debug(f'data file path: {file_path}')
            filename = file_path.name
            parts = file_path.parts
            source_type = parts[self.data_source_type_index]
            year = parts[self.data_year_index]
            month = parts[self.data_month_index]
            day = parts[self.data_day_index]
            source_id = DataFilename(filename).source_id()
            log.debug(f'type: {source_type} Y: {year} M: {month} D: {day} id: {source_id} file: {filename}')
            output_path = pathlib.Path(self.out_path, source_type, year, month, day, source_id)
            data_path = pathlib.Path(output_path, 'data')
            target_path = pathlib.Path(data_path, filename)
            link(file_path, target_path)
            yield {'source_id': source_id, 'output_path': output_path}
