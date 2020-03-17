import os

import environs
import structlog
import pathlib

import lib.log_config as log_config
import lib.file_linker as file_linker
import lib.file_crawler as file_crawler

log = structlog.get_logger()


def get_data_files(data_path, out_path):
    keys = []
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
    return keys


def process_location_files(location_path, keys, out_path, output_directories,
                           empty_data_path, empty_flags_path, empty_uncertainty_data_path):
    for file_path in file_crawler.crawl(location_path):
        parts = file_path.parts
        source_type = parts[3]
        year = parts[4]
        month = parts[5]
        day = parts[6]
        named_location_name = parts[7]
        log.debug(f'named location name: {named_location_name}')
        filename = parts[8]
        target_root = os.path.join(out_path, source_type, year, month, day, named_location_name)
        # link the location file into the output directory
        location_target = os.path.join(target_root, 'location', filename)
        file_linker.link(file_path, location_target)
        # create an empty calibration file in the target directory but do not overwrite
        calibration_target = os.path.join(target_root, 'calibration')
        os.makedirs(calibration_target, exist_ok=True)
        # create key to find corresponding data for the sensor and date
        key = '/' + source_type + '/' + year + '/' + month + '/' + day + '/' + named_location_name
        if key not in keys:
            # key not found, create empty directories and files
            print(f'Key not found {key}')
            for directory in output_directories:
                target_dir = os.path.join(target_root, directory)
                if directory == 'data':
                    link_path(target_dir, empty_data_path, named_location_name, year, month, day)
                elif directory == 'flags':
                    link_path(target_dir, empty_flags_path, named_location_name, year, month, day)
                elif directory == 'uncertainty_data':
                    link_path(target_dir, empty_uncertainty_data_path, named_location_name, year, month, day)
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

    keys = get_data_files(data_path, out_path)
    process_location_files(location_path, keys, out_path, output_directories,
                           empty_data_path, empty_flags_path, empty_uncertainty_data_path)


if __name__ == '__main__':
    main()
