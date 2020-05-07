#!/usr/bin/env python3
import os

import structlog

from lib.file_linker import link
from lib.file_crawler import crawl

from data_gap_filler.date_between import is_date_between

log = structlog.get_logger()


def write_data_files(data_path,
                     out_path,
                     source_type_index,
                     year_index,
                     month_index,
                     day_index,
                     location_index,
                     data_type_index,
                     filename_index,
                     start_date=None,
                     end_date=None):
    """
    Return all data file path keys between the start and end dates.

    :param data_path: The path to the data file directory.
    :type data_path: str
    :param out_path: The path to write results.
    :type out_path: str
    :param source_type_index: The source type index in the file path.
    :type source_type_index: int
    :param year_index: The year index in the file path.
    :type year_index: int
    :param month_index: The month index in the file path.
    :type month_index: int
    :param day_index: The dah index in the file path.
    :type day_index: int
    :param location_index: The location index in the file path.
    :type location_index: int
    :param data_type_index: The data type index in the file path.
    :type data_type_index: int
    :param filename_index: The filename index in the file path.
    :type filename_index: int
    :param start_date: The start date.
    :type start_date: datetime object
    :param end_date: The end date.
    :type end_date: datetime object
    :return: list of data files.
    """
    for file_path in crawl(data_path):
        parts = file_path.parts
        source_type = parts[source_type_index]
        year = parts[year_index]
        month = parts[month_index]
        day = parts[day_index]
        location = parts[location_index]
        data_type = parts[data_type_index]
        filename = parts[filename_index]
        if not is_date_between(year, month, day, start_date, end_date):
            continue
        path = os.path.join(out_path, source_type, year, month, day, location, data_type, filename)
        link(file_path, path)
