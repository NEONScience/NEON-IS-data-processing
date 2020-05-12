#!/usr/bin/env python3
import pathlib
import os

import environs
import structlog

import lib.log_config as log_config
from lib.file_linker import link
from lib.file_crawler import crawl

log = structlog.get_logger()


def group(data_path,
          location_path,
          out_path,
          source_type_index,
          year_index,
          month_index,
          day_index,
          source_id_index,
          filename_index):
    """
    Write event data and location files into output path.

    :param data_path: The path to the data files.
    :type data_path: str
    :param location_path: The path to the location file.
    :type location_path: str
    :param out_path: The path for writing results.
    :type out_path: str
    :param source_type_index: The input file path index of the source type.
    :type source_type_index: int
    :param year_index: The input file path index of the year.
    :type year_index: int
    :param month_index: The input file path index of the month.
    :type month_index: int
    :param day_index: The input file path index of the day.
    :type day_index: int
    :param source_id_index: The input file path index of the source ID.
    :type source_id_index: int
    :param filename_index: The input file path index of the filename.
    :type filename_index: int
    :return:
    """
    for file_path in crawl(data_path):
        parts = pathlib.Path(file_path).parts
        source_type = parts[source_type_index]
        year = parts[year_index]
        month = parts[month_index]
        day = parts[day_index]
        source_id = parts[source_id_index]
        filename = parts[filename_index]
        log.debug(f'filename: {filename} source type: {source_type} source_id: {source_id}')
        target_root = os.path.join(out_path, source_type, year, month, day, source_id)
        link_location(location_path, target_root)
        data_target_path = os.path.join(target_root, 'data', filename)
        log.debug(f'data_target_path: {data_target_path}')
        link(file_path, data_target_path)


def link_location(location_path, target_root):
    """
    Link the location file into the target directory.

    :param location_path: The location file path.
    :type location_path: str
    :param target_root: The target directory path.
    :type target_root: str
    :return:
    """
    for file in crawl(location_path):
        location_filename = pathlib.Path(file).name
        location_target_path = os.path.join(target_root, 'location', location_filename)
        log.debug(f'location_target_path: {location_target_path}')
        link(file, location_target_path)


def main():
    env = environs.Env()
    data_path = env.str('DATA_PATH')
    location_path = env.str('LOCATION_PATH')
    out_path = env.str('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL')
    source_type_index = env.int('SOURCE_TYPE_INDEX')
    year_index = env.int('YEAR_INDEX')
    month_index = env.int('MONTH_INDEX')
    day_index = env.int('DAY_INDEX')
    source_id_index = env.int('SOURCE_ID_INDEX')
    filename_index = env.int('FILENAME_INDEX')
    log_config.configure(log_level)
    log.debug(f'data_dir: {data_path} location_dir: {location_path} out_dir: {out_path}')
    group(data_path,
          location_path,
          out_path,
          source_type_index,
          year_index,
          month_index,
          day_index,
          source_id_index,
          filename_index)


if __name__ == '__main__':
    main()
