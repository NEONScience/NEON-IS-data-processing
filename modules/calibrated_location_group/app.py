#!/usr/bin/env python3
import pathlib
import os

import environs
import structlog

import lib.log_config as log_config
import lib.file_linker as file_linker
import lib.file_crawler as file_crawler

log = structlog.get_logger()


def group(calibrated_path, location_path, out_path):
    """Write calibrated data and location files into output path."""
    i = 0
    for file_path in file_crawler.crawl(calibrated_path):
        parts = file_path.parts
        source_type = parts[3]
        year = parts[4]
        month = parts[5]
        day = parts[6]
        source_id = parts[7]
        data_type = parts[8]
        log.debug(f'year: {year}  month: {month}  day: {day}')
        log.debug(f'source type: {source_type} source_id: {source_id} data type: {data_type}')
        target_root = os.path.join(out_path, source_type, year, month, day, source_id)
        if i == 0:  # Only link location once.
            link_location(location_path, target_root)
        # Grab all directories and files under the common path (after the data type).
        target = os.path.join(target_root, data_type, *parts[9:])
        log.debug(f'target: {target}')
        file_linker.link(file_path, target)
        i += 1


def link_location(location_path, target_root):
    for file in file_crawler.crawl(location_path):
        location_filename = pathlib.Path(file).name
        target = os.path.join(target_root, 'location', location_filename)
        file_linker.link(file, target)


def main():
    env = environs.Env()
    calibrated_path = env('CALIBRATED_PATH')
    location_path = env('LOCATION_PATH')
    out_path = env('OUT_PATH')
    log_level = env('LOG_LEVEL')
    log_config.configure(log_level)
    log.debug(f'calibrated_dir: {calibrated_path} '
              f'location_dir: {location_path} out_dir: {out_path}')
    group(calibrated_path, location_path, out_path)


if __name__ == '__main__':
    main()
