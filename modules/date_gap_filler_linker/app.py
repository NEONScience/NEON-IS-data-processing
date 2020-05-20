#!/usr/bin/env python3
import os
import structlog
import environs
import glob
from pathlib import Path

import lib.log_config as log_config
from lib.file_crawler import crawl
from lib.file_linker import link


log = structlog.get_logger()


def main():
    env = environs.Env()
    in_path = env.path('IN_PATH')
    out_path = env.path('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL', 'INFO')
    source_type_index = env.int('SOURCE_TYPE_INDEX')
    year_index = env.int('YEAR_INDEX')
    month_index = env.int('MONTH_INDEX')
    day_index = env.int('DAY_INDEX')
    location_index = env.int('LOCATION_INDEX')
    data_type_index = env.int('DATA_TYPE_INDEX')
    filename_index = env.int('FILENAME_INDEX')
    log_config.configure(log_level)

    suffix = '.empty'

    for file in crawl(in_path):
        parts = Path(file).parts
        root = os.path.join(*parts[:source_type_index])
        source = parts[source_type_index]
        year = parts[year_index]
        month = parts[month_index]
        day = parts[day_index]
        location = parts[location_index]
        data_type = parts[data_type_index]
        filename = parts[filename_index]
        if file.suffix == suffix:
            if os.path.exists(file.stem):
                continue  # a real file exists, do not link the empty file
            else:
                filename = filename.replace(suffix, '')  # no real file exists, trim the suffix
        pathname = f'{root}/{source}/{year}/{month}/{day}/{location}/*/*{suffix}'
        if glob.glob(pathname):  # if empty file exists location is active, link the file
            link_path = os.path.join(out_path, source, year, month, day, location, data_type, filename)
            log.debug(f'file: {file} link: {link_path}')
            link(file, link_path)


if __name__ == '__main__':
    main()
