#!/usr/bin/env python3
from pathlib import Path

import structlog

from date_gap_filler.date_between import date_between
from date_gap_filler.empty_file_handler import EmptyFiles, EmptyFileLinker
from date_gap_filler.app_config import AppConfig

log = structlog.get_logger()


def link_location_files(config: AppConfig):
    """
    Process the location files and fill date gaps with empty files.

    :param config: The application configuration.
    """
    out_path = config.out_path
    output_directories = config.output_directories
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
    empty_files = EmptyFiles(config.empty_files_path, config.empty_file_type_index)

    write_data = True if 'data' in output_directories else False
    write_flags = True if 'flags' in output_directories else False
    write_uncertainty_data = True if 'uncertainty_data' in output_directories else False
    write_uncertainty_coefficient = True if 'uncertainty_coef' in output_directories else False
    write_calibration = True if 'calibration' in output_directories else False

    for path in location_path.rglob('*'):
        if path.is_file():
            parts = path.parts
            source_type = parts[source_type_index]
            year = parts[year_index]
            month = parts[month_index]
            day = parts[day_index]
            location = parts[location_index]
            filename = parts[filename_index]
            if not date_between(int(year), int(month), int(day), start_date, end_date):
                continue
            root_output_path = Path(out_path, source_type, year, month, day, location)
            location_dir = Path(root_output_path, 'location')
            location_dir.mkdir(parents=True, exist_ok=True)
            location_link = Path(location_dir, filename)
            location_link.symlink_to(path)
            empty_file_linker = EmptyFileLinker(empty_files, location, year, month, day)
            if write_data:
                data_dir = Path(root_output_path, 'data')
                empty_file_linker.link_data_file(data_dir)
            if write_flags:
                flag_dir = Path(root_output_path, 'flags')
                empty_file_linker.link_flags_file(flag_dir)
            if write_uncertainty_data:
                uncertainty_dir = Path(root_output_path, 'uncertainty_data')
                empty_file_linker.link_uncertainty_file(uncertainty_dir)
            if write_uncertainty_coefficient:
                Path(root_output_path, 'uncertainty_coef').mkdir(parents=True, exist_ok=True)
            if write_calibration:
                Path(root_output_path, 'calibration').mkdir(parents=True, exist_ok=True)
