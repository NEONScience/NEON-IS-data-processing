#!/usr/bin/env python3
from pathlib import Path

import environs
import structlog

import lib.log_config as log_config
from lib.file_crawler import crawl

log = structlog.get_logger()


def group(data_path: Path,
          location_path: Path,
          out_path: Path,
          source_type_index: int,
          year_index: int,
          month_index: int,
          day_index: int,
          source_id_index: int,
          filename_index: int):
    """
    Link event data and location files into output path.

    :param data_path: The path to the data files.
    :param location_path: The path to the location file.
    :param out_path: The path for writing results.
    :param source_type_index: The input file path index of the source type.
    :param year_index: The input file path index of the year.
    :param month_index: The input file path index of the month.
    :param day_index: The input file path index of the day.
    :param source_id_index: The input file path index of the source ID.
    :param filename_index: The input file path index of the filename.
    """
    for path in crawl(data_path):
        parts = path.parts
        source_type = parts[source_type_index]
        year = parts[year_index]
        month = parts[month_index]
        day = parts[day_index]
        source_id = parts[source_id_index]
        filename = parts[filename_index]
        log.debug(f'filename: {filename} source type: {source_type} source_id: {source_id}')
        link_root_path = Path(out_path, source_type, year, month, day, source_id)
        link_location(link_root_path, location_path)
        link_path = Path(link_root_path, 'data', filename)
        log.debug(f'data link: {link_path}')
        link_path.parent.mkdir(parents=True, exist_ok=True)
        link_path.symlink_to(path)


def link_location(link_root_path: Path, location_path: Path):
    """
    Link the location file into the target directory.

    :param location_path: The location file path.
    :param link_root_path: The target directory path.
    :return:
    """
    for path in crawl(location_path):
        link_path = Path(link_root_path, 'location', path.name)
        log.debug(f'location link: {link_path}')
        link_path.parent.mkdir(parents=True, exist_ok=True)
        link_path.symlink_to(path)


def main():
    env = environs.Env()
    data_path = env.path('DATA_PATH')
    location_path = env.path('LOCATION_PATH')
    out_path = env.path('OUT_PATH')
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
