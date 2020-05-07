#!/usr/bin/env python3
import pathlib
import os

import structlog

from lib.file_linker import link
from lib.file_crawler import crawl
from lib.data_filename import DataFilename

log = structlog.get_logger()


def link_data(data_path,
              out_path,
              source_type_index,
              year_index,
              month_index,
              day_index,
              file_index):
    """
    Link data files into the output path and yield the output directory.

    :param data_path: The path to the data files.
    :type data_path: str
    :param out_path: The output path to write grouped files.
    :type out_path: str
    :param source_type_index: The file path source type index.
    :type source_type_index: int
    :param year_index: The file path year index.
    :type year_index: int
    :param month_index: The file path month index.
    :type month_index: int
    :param day_index: The file path day index.
    :type day_index: int
    :param file_index: The file path file index.
    :type file_index: int
    :return: Yields the output directory for each data file.
    """
    for file in crawl(data_path):
        parts = file.parts
        source_type = parts[source_type_index]
        year = parts[year_index]
        month = parts[month_index]
        day = parts[day_index]
        filename = parts[file_index]
        source_id = DataFilename(filename).source_id()
        output_dir = os.path.join(out_path, source_type, year, month, day, source_id)
        output_path = os.path.join(output_dir, 'data', filename)
        log.debug(f'data output path: {output_path}')
        link(file, output_path)
        yield output_dir


def link_location(location_path, output_dir):
    """
    Link the location file.

    :param location_path: The path to the file.
    :type location_path str
    :param output_dir: The output directory.
    :type output_dir: str
    :return:
    """
    for file in crawl(location_path):
        location_filename = pathlib.Path(file).name
        output_path = os.path.join(output_dir, 'location', location_filename)
        log.debug(f'location output path: {output_path}')
        link(file, output_path)
