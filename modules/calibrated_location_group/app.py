#!/usr/bin/env python3
import pathlib
import os

import environs
import structlog

import lib.log_config as log_config
import lib.file_linker as file_linker
import lib.file_crawler as file_crawler

log = structlog.get_logger()


def group(calibrated_path, location_path, out_path, source_type_index, year_index,
          month_index, day_index, source_id_index, data_type_index):
    """
    Write calibrated data and location files into the output path.

    :param calibrated_path: Path to the calibration files to process.
    :type calibrated_path: str
    :param location_path: Path to the calibration files to process.
    :type location_path: str
    :param out_path: Directory path to write output.
    :type out_path: str
    :param source_type_index: The source type index in the calibrated file path.
    :type source_type_index: int
    :param year_index: The year index in the calibrated file path.
    :type year_index: int
    :param month_index: The month index in the calibrated file path.
    :type month_index: int
    :param day_index: The day index in the calibrated file path.
    :type day_index: int
    :param source_id_index: The source ID index in the calibrated file path.
    :type source_id_index: int
    :param data_type_index: The data type index in the calibrated file path.
    :type data_type_index: int
    :return:
    """
    i = 0
    for file_path in file_crawler.crawl(calibrated_path):
        parts = file_path.parts
        source_type = parts[source_type_index]
        year = parts[year_index]
        month = parts[month_index]
        day = parts[day_index]
        source_id = parts[source_id_index]
        data_type = parts[data_type_index]
        log.debug(f'year: {year}  month: {month}  day: {day}')
        log.debug(f'source type: {source_type} source_id: {source_id} data type: {data_type}')
        target_root = os.path.join(out_path, source_type, year, month, day, source_id)
        if i == 0:  # only link location once
            link_location(location_path, target_root)
        # pass remainder of file path after the data type
        target = os.path.join(target_root, data_type, *parts[9:])
        log.debug(f'target: {target}')
        file_linker.link(file_path, target)
        i += 1


def link_location(location_path, target_root):
    """
    Link the location file into the target root.

    :param location_path: The location file path.
    :type location_path: str
    :param target_root: The target directory to write the location file.
    :type target_root: str
    :return:
    """
    for file in file_crawler.crawl(location_path):
        location_filename = pathlib.Path(file).name
        target = os.path.join(target_root, 'location', location_filename)
        file_linker.link(file, target)


def main():
    env = environs.Env()
    calibrated_path = env.str('CALIBRATED_PATH')
    location_path = env.str('LOCATION_PATH')
    out_path = env.str('OUT_PATH')
    log_level = env.str('LOG_LEVEL', 'INFO')
    source_type_index = env.int('SOURCE_TYPE_INDEX')
    year_index = env.int('YEAR_INDEX')
    month_index = env.int('MONTH_INDEX')
    day_index = env.int('DAY_INDEX')
    source_id_index = env.int('SOURCE_ID_INDEX')
    data_type_index = env.int('DATA_TYPE_INDEX')
    log_config.configure(log_level)
    log.debug(f'calibrated_dir: {calibrated_path} '
              f'location_dir: {location_path} out_dir: {out_path}')
    group(calibrated_path, location_path, out_path, source_type_index, year_index,
          month_index, day_index, source_id_index, data_type_index)


if __name__ == '__main__':
    main()
