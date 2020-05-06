#!/usr/bin/env python3
import sys
from datetime import datetime

import environs
import structlog

import lib.log_config as log_config

from data_gap_filler.data_file_handler import write_data_files
from data_gap_filler.location_file_handler import write_location_files
from data_gap_filler import empty_file_handler as empty_file_handler

log = structlog.get_logger()


def main():
    env = environs.Env()
    data_path = env.str('DATA_PATH', None)
    location_path = env.str('LOCATION_PATH', None)
    empty_files_path = env.str('EMPTY_FILES_PATH')
    output_directories = env.str('OUTPUT_DIRECTORIES')
    start_date = env.str('START_DATE', None)
    end_date = env.str('END_DATE', None)
    out_path = env.str('OUT_PATH')
    log_level = env.str('LOG_LEVEL', 'INFO')
    data_source_type_index = env.int('DATA_SOURCE_TYPE_INDEX')
    data_year_index = env.int('DATA_YEAR_INDEX')
    data_month_index = env.int('DATA_MONTH_INDEX')
    data_day_index = env.int('DATA_DAY_INDEX')
    data_location_index = env.int('DATA_LOCATION_INDEX')
    data_type_index = env.int('DATA_TYPE_INDEX')
    data_filename_index = env.int('DATA_FILENAME_INDEX')
    location_source_type_index = env.int('LOCATION_SOURCE_TYPE_INDEX')
    location_year_index = env.int('LOCATION_YEAR_INDEX')
    location_month_index = env.int('LOCATION_MONTH_INDEX')
    location_day_index = env.int('LOCATION_DAY_INDEX')
    location_index = env.int('LOCATION_INDEX')
    location_filename_index = env.int('LOCATION_FILENAME_INDEX')
    empty_file_type_index = env.int('EMPTY_FILE_TYPE_INDEX')

    log_config.configure(log_level)

    # directory names to output are a comma separated string
    if ',' in output_directories:
        output_directories = output_directories.split(',')

    # parse dates from strings
    date_format = '%Y-%m-%d'
    if start_date is not None:
        start_date = datetime.strptime(start_date, date_format)
    if end_date is not None:
        end_date = datetime.strptime(end_date, date_format)

    # empty file paths
    empty_files_paths = empty_file_handler.get_paths(empty_files_path, empty_file_type_index)
    empty_data_path = empty_files_paths.get('empty_data_path')
    empty_flags_path = empty_files_paths.get('empty_flags_path')
    empty_uncertainty_data_path = empty_files_paths.get('empty_uncertainty_data_path')
    if empty_data_path is None:
        log.error('Empty data file not found.')
        sys.exit(1)
    if empty_flags_path is None:
        log.error('Empty flags file not found.')
        sys.exit(1)
    if empty_uncertainty_data_path is None:
        log.error('Empty uncertainty data file not found.')
        sys.exit(1)

    if data_path is not None:
        write_data_files(data_path,
                         out_path,
                         data_source_type_index,
                         data_year_index,
                         data_month_index,
                         data_day_index,
                         data_location_index,
                         data_type_index,
                         data_filename_index,
                         start_date=start_date,
                         end_date=end_date)
    if location_path is not None:
        write_location_files(location_path,
                             out_path,
                             output_directories,
                             empty_data_path,
                             empty_flags_path,
                             empty_uncertainty_data_path,
                             location_source_type_index,
                             location_year_index,
                             location_month_index,
                             location_day_index,
                             location_index,
                             location_filename_index,
                             start_date=start_date,
                             end_date=end_date)


if __name__ == '__main__':
    main()
