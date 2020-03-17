import os

import environs
import structlog
import pathlib
import cx_Oracle
from contextlib import closing

import lib.log_config as log_config
import lib.file_linker as file_linker
import lib.file_crawler as file_crawler
import data_access.named_location_finder as named_location_finder

log = structlog.get_logger()


def get_data_files(data_path, out_path):
    keys = []
    source_type = None
    for file_path in file_crawler.crawl(data_path):
        parts = file_path.parts
        source_type = parts[3]
        year = parts[4]
        month = parts[5]
        day = parts[6]
        location_name = parts[7]
        data_type = parts[8]
        filename = parts[9]
        target_root = os.path.join(out_path, source_type, year, month, day, location_name)
        target_path = os.path.join(target_root, data_type, filename)
        file_linker.link(file_path, target_path)
        key = '/' + source_type + '/' + year + '/' + month + '/' + day + '/' + location_name
        keys.append(key)
    return {'source_type': source_type, 'keys': keys}


def process_location_files(connection, location_path, data_file_keys, out_path, output_directories,
                           empty_data_path, empty_flags_path, empty_uncertainty_data_path):
    source_type = data_file_keys.get('source_type')
    keys = data_file_keys.get('keys')
    for file_path in file_crawler.crawl(location_path):
        parts = file_path.parts
        year = parts[3]
        month = parts[4]
        day = parts[5]
        location_name = parts[6]
        filename = parts[7]
        # get the type of sensor bound to this named location
        schema_name = named_location_finder.get_schema_name(connection, location_name)
        if schema_name is None:
            log.error(f'Schema name not found for named location: {location_name}')
            continue
        if schema_name != source_type:
            continue  # Not the same type of sensor specified by the data files.
        target_root = os.path.join(out_path, schema_name, year, month, day, location_name)
        # link the location file into the output directory
        location_target = os.path.join(target_root, 'location', filename)
        file_linker.link(file_path, location_target)
        # create an empty calibration file in the target directory but do not overwrite
        calibration_target = os.path.join(target_root, 'calibration')
        os.makedirs(calibration_target, exist_ok=True)
        # create key to find corresponding data for the sensor and date
        key = '/' + schema_name + '/' + year + '/' + month + '/' + day + '/' + location_name
        if key not in keys:
            # key not found, create empty directories and files
            print(f'Key not found {key}')
            for directory in output_directories:
                target_dir = os.path.join(target_root, directory)
                if directory == 'data':
                    link_path(target_dir, empty_data_path, location_name, year, month, day)
                elif directory == 'flags':
                    link_path(target_dir, empty_flags_path, location_name, year, month, day)
                elif directory == 'uncertainty_data':
                    link_path(target_dir, empty_uncertainty_data_path, location_name, year, month, day)
                elif directory == 'uncertainty_coef':
                    os.makedirs(target_dir, exist_ok=True)


def link_path(target_dir, empty_file_path, location_name, year, month, day):
    file_name = pathlib.Path(empty_file_path).name
    file_name = file_name.replace('location', location_name)
    file_name = file_name.replace('year', year)
    file_name = file_name.replace('month', month)
    file_name = file_name.replace('day', day)
    target_path = os.path.join(target_dir, file_name)
    print(f'target_path: {target_path}')
    file_linker.link(empty_file_path, target_path)


def main():
    env = environs.Env()
    data_path = env('DATA_PATH')
    location_path = env('LOCATION_PATH')
    empty_files_path = env('EMPTY_FILES_PATH')
    output_directories = env('OUTPUT_DIRECTORIES')
    db_url = env('DATABASE_URL')
    out_path = env('OUT_PATH')
    log_level = env('LOG_LEVEL')
    log_config.configure(log_level)

    # Directory names to output should be a comma separated string.
    if ',' in output_directories:
        output_directories = output_directories.split(',')

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

    with closing(cx_Oracle.connect(db_url)) as connection:
        data_file_keys = get_data_files(data_path, out_path)
        process_location_files(connection, location_path, data_file_keys, out_path, output_directories,
                               empty_data_path, empty_flags_path, empty_uncertainty_data_path)


if __name__ == '__main__':
    main()
