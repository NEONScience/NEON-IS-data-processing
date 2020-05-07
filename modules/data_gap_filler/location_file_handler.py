#!/usr/bin/env python3
import os

import structlog
import pathlib

from lib.file_linker import link
from lib.file_crawler import crawl

from data_gap_filler.date_between import is_date_between
from data_gap_filler.empty_file_handler import render_empty_file_name, link_empty_file

log = structlog.get_logger()


def write_location_files(location_path,
                         out_path,
                         output_directories,
                         empty_data_file_path,
                         empty_flags_file_path,
                         empty_uncertainty_data_file_path,
                         source_type_index,
                         year_index,
                         month_index,
                         day_index,
                         location_index,
                         filename_index,
                         start_date=None,
                         end_date=None):
    """
    Process the location files and fill date gaps with empty files.

    :param location_path: The path to the location file.
    :type location_path: str
    :param out_path: The path to write results.
    :type out_path: str
    :param output_directories: The output directories to write.
    :type output_directories: list
    :param empty_data_file_path: Path to the empty data files.
    :type empty_data_file_path: str
    :param empty_flags_file_path: Path to the empty flag files.
    :type empty_flags_file_path: str
    :param empty_uncertainty_data_file_path: Path to the empty uncertainty data file.
    :type empty_uncertainty_data_file_path: str
    :param source_type_index: The source type index in the file path.
    :type source_type_index: int
    :param year_index: The year index in the file path.
    :type year_index: int
    :param month_index: The month index in the file path.
    :type month_index: int
    :param day_index: The day index in the file path.
    :type day_index: int
    :param location_index: The location index in the file path.
    :type location_index: int
    :param filename_index: The filename index in the file path.
    :type filename_index: int
    :param start_date: The start date.
    :type start_date datetime object
    :param end_date: The end date.
    :type end_date: datetime object
    :return:
    """
    for file_path in crawl(location_path):
        parts = file_path.parts
        source_type = parts[source_type_index]
        year = parts[year_index]
        month = parts[month_index]
        day = parts[day_index]
        location = parts[location_index]
        filename = parts[filename_index]
        if not is_date_between(year, month, day, start_date, end_date):
            continue
        target_root = os.path.join(out_path, source_type, year, month, day, location)
        link(file_path, os.path.join(target_root, 'location', filename))
        if 'data' in output_directories:
            data_dir = os.path.join(target_root, 'data')
            if not os.path.exists(data_dir):
                empty_name = pathlib.Path(empty_data_file_path).name
                filename = render_empty_file_name(empty_name, location, year, month, day)
                link_empty_file(data_dir, empty_data_file_path, filename)
        if 'flags' in output_directories:
            flag_dir = os.path.join(target_root, 'flags')
            if not os.path.exists(flag_dir):
                empty_name = pathlib.Path(empty_flags_file_path).name
                filename = render_empty_file_name(empty_name, location, year, month, day)
                link_empty_file(flag_dir, empty_flags_file_path, filename)
        if 'uncertainty_data' in output_directories:
            uncertainty_dir = os.path.join(target_root, 'uncertainty_data')
            if not os.path.exists(uncertainty_dir):
                empty_name = pathlib.Path(empty_uncertainty_data_file_path).name
                filename = render_empty_file_name(empty_name, location, year, month, day)
                link_empty_file(uncertainty_dir, empty_uncertainty_data_file_path, filename)
        if 'uncertainty_coef' in output_directories:
            coefficient_dir = os.path.join(target_root, 'uncertainty_coef')
            os.makedirs(coefficient_dir, exist_ok=True)
        if 'calibration' in output_directories:
            calibration_dir = os.path.join(target_root, 'calibration')
            os.makedirs(calibration_dir, exist_ok=True)
