#!/usr/bin/env python3
from pathlib import Path

import environs
import structlog

import lib.log_config as log_config
from lib.file_crawler import crawl
from lib.file_linker import link

log = structlog.get_logger()


def group(calibrated_path: Path,
          location_path: Path,
          out_path: Path,
          source_type_index: int,
          year_index: int,
          month_index: int,
          day_index: int,
          source_id_index: int,
          data_type_index: int):
    """
    Write calibrated data and location files into the output path.

    :param calibrated_path: Path to the calibration files to process.
    :param location_path: Path to the calibration files to process.
    :param out_path: Directory path to write output.
    :param source_type_index: The source type index in the calibrated file path.
    :param year_index: The year index in the calibrated file path.
    :param month_index: The month index in the calibrated file path.
    :param day_index: The day index in the calibrated file path.
    :param source_id_index: The source ID index in the calibrated file path.
    :param data_type_index: The data type index in the calibrated file path.
    """
    make_location_link = True
    for path in crawl(calibrated_path):
        parts = path.parts
        source_type = parts[source_type_index]
        year = parts[year_index]
        month = parts[month_index]
        day = parts[day_index]
        source_id = parts[source_id_index]
        data_type = parts[data_type_index]
        log.debug(f'year: {year}  month: {month}  day: {day} source type: {source_type} '
                  f'source_id: {source_id} data type: {data_type}')
        root_link_path = Path(out_path, source_type, year, month, day, source_id)
        # only link location files once
        if make_location_link:
            link_location(location_path, root_link_path)
            make_location_link = False
        # pass remainder of file path after the data type
        link_path = Path(root_link_path, data_type, *parts[data_type_index+1:])
        link(path, link_path)


def link_location(location_path: Path, root_link_path: Path):
    """
    Link the location file into the target root.

    :param location_path: The location file path.
    :param root_link_path: The target directory to write the location file.
    """
    for path in crawl(location_path):
        link_path = Path(root_link_path, 'location', path.name)
        link(path, link_path)


def main():
    env = environs.Env()
    calibrated_path = env.path('CALIBRATED_PATH')
    location_path = env.path('LOCATION_PATH')
    out_path = env.path('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL', 'INFO')
    source_type_index = env.int('SOURCE_TYPE_INDEX')
    year_index = env.int('YEAR_INDEX')
    month_index = env.int('MONTH_INDEX')
    day_index = env.int('DAY_INDEX')
    source_id_index = env.int('SOURCE_ID_INDEX')
    data_type_index = env.int('DATA_TYPE_INDEX')
    log_config.configure(log_level)
    log.debug(f'calibrated_path: {calibrated_path} location_path: {location_path} out_path: {out_path}')
    group(calibrated_path, location_path, out_path, source_type_index, year_index,
          month_index, day_index, source_id_index, data_type_index)


if __name__ == '__main__':
    main()
