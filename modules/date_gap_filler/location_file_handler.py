#!/usr/bin/env python3
import os

import structlog

from lib.file_linker import link
from lib.file_crawler import crawl

from date_gap_filler.date_between import date_between
from date_gap_filler.empty_file_handler import link_empty_file

log = structlog.get_logger()


def link_location_files(config):
    """
    Process the location files and fill date gaps with empty files.

    :param config: The application configuration.
    :type config: data_gap_filler.app_config.AppConfig
    :return:
    """
    # path indices
    location_path = config.location_path
    source_type_index = config.location_source_type_index
    year_index = config.location_year_index
    month_index = config.location_month_index
    day_index = config.location_day_index
    location_index = config.location_index
    filename_index = config.location_filename_index
    # dates
    start_date = config.start_date
    end_date = config.end_date
    # empty file paths
    empty_data_path = config.empty_data_path
    empty_flags_path = config.empty_flags_path
    empty_uncertainty_path = config.empty_uncertainty_data_path

    for file_path in crawl(location_path):
        parts = file_path.parts
        source_type = parts[source_type_index]
        year = parts[year_index]
        month = parts[month_index]
        day = parts[day_index]
        location = parts[location_index]
        filename = parts[filename_index]
        if not date_between(int(year), int(month), int(day), start_date, end_date):
            continue
        link_root = os.path.join(config.out_path, source_type, year, month, day, location)
        link(file_path, os.path.join(link_root, 'location', filename))
        if 'data' in config.output_directories:
            data_dir = os.path.join(link_root, 'data')
            link_empty_file(data_dir, empty_data_path, location, year, month, day)
        if 'flags' in config.output_directories:
            flag_dir = os.path.join(link_root, 'flags')
            link_empty_file(flag_dir, empty_flags_path, location, year, month, day)
        if 'uncertainty_data' in config.output_directories:
            uncertainty_dir = os.path.join(link_root, 'uncertainty_data')
            link_empty_file(uncertainty_dir, empty_uncertainty_path, location, year, month, day)
        if 'uncertainty_coef' in config.output_directories:
            coefficient_dir = os.path.join(link_root, 'uncertainty_coef')
            os.makedirs(coefficient_dir, exist_ok=True)
        if 'calibration' in config.output_directories:
            calibration_dir = os.path.join(link_root, 'calibration')
            os.makedirs(calibration_dir, exist_ok=True)
