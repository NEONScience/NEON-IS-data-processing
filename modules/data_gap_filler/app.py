import os
from datetime import datetime

import environs
import structlog
import pathlib

import lib.log_config as log_config
import lib.file_crawler as file_crawler

from data_gap_filler import gap_filler as gap_filler

log = structlog.get_logger()


def get_date_constraints():
    date_format = '%Y-%m-%d'
    try:
        start_date = os.environ['START_DATE']
        end_date = os.environ['END_DATE']
    except KeyError:
        print(f'Proceeding without start and end dates.')
        return None
    if start_date is not None:
        start_date = datetime.strptime(start_date, date_format)
    if end_date is not None:
        end_date = datetime.strptime(end_date, date_format)
    return {'start_date': start_date, 'end_date': end_date}


def get_empty_file_paths(empty_files_path):
    empty_data_path = None
    empty_flags_path = None
    empty_uncertainty_data_path = None
    for file_path in file_crawler.crawl(empty_files_path):
        parts = pathlib.Path(file_path).parts
        trimmed = parts[3:]
        directory_name = trimmed[1]
        if 'data' == directory_name:
            empty_data_path = file_path
        elif 'flags' == directory_name:
            empty_flags_path = file_path
        elif 'uncertainty_data' == directory_name:
            empty_uncertainty_data_path = file_path
    if empty_data_path is None:
        log.error('Empty data file not found.')
        exit(1)
    if empty_flags_path is None:
        log.error('Empty flags file not found.')
        exit(1)
    if empty_uncertainty_data_path is None:
        log.error('Empty uncertainty data file not found.')
        exit(1)
    return {'empty_data_path': empty_data_path,
            'empty_flags_path': empty_flags_path,
            'empty_uncertainty_data_path': empty_uncertainty_data_path}


def main():
    env = environs.Env()
    data_path = env('DATA_PATH')
    location_path = env('LOCATION_PATH')
    empty_files_path = env('EMPTY_FILES_PATH')
    output_directories = env('OUTPUT_DIRECTORIES')
    out_path = env('OUT_PATH')
    log_level = env('LOG_LEVEL')
    log_config.configure(log_level)

    # directory names to output should be a comma separated string.
    if ',' in output_directories:
        output_directories = output_directories.split(',')

    # empty file paths
    empty_files_paths = get_empty_file_paths(empty_files_path)
    empty_data_path = empty_files_paths.get('empty_data_path')
    empty_flags_path = empty_files_paths.get('empty_flags_path')
    empty_uncertainty_data_path = empty_files_paths.get('empty_uncertainty_data_path')

    date_constraints = get_date_constraints()
    if date_constraints is not None:
        start_date = date_constraints.get('start_date')
        end_date = date_constraints.get('end_date')
        keys = gap_filler.get_data_files(data_path, out_path, start_date=start_date, end_date=end_date)
        gap_filler.process_location_files(location_path, keys, out_path, output_directories,
                                          empty_data_path, empty_flags_path, empty_uncertainty_data_path,
                                          start_date=start_date, end_date=end_date)
    else:
        keys = gap_filler.get_data_files(data_path, out_path)
        gap_filler.process_location_files(location_path, keys, out_path, output_directories,
                                          empty_data_path, empty_flags_path, empty_uncertainty_data_path)


if __name__ == '__main__':
    main()
