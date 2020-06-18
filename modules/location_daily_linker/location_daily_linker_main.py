#!/usr/bin/env python3
import environs
import structlog

import common.log_config as log_config
from location_daily_linker.location_file_path import LocationFilePath
from location_daily_linker.location_daily_linker import LocationDailyLinker


log = structlog.get_logger()


def main():
    env = environs.Env()
    location_path = env.path('LOCATION_PATH')
    out_path = env.path('OUT_PATH')
    source_type_index = env.int('SOURCE_TYPE_INDEX')
    year_index = env.int('YEAR_INDEX')
    month_index = env.int('MONTH_INDEX')
    location_index = env.int('LOCATION_INDEX')
    log_level = env.log_level('LOG_LEVEL', 'INFO')
    log_config.configure(log_level)
    location_file_path = LocationFilePath(source_type_index=source_type_index,
                                          year_index=year_index,
                                          month_index=month_index,
                                          location_index=location_index)
    location_daily_linker = LocationDailyLinker(location_path=location_path, out_path=out_path,
                                                location_file_path=location_file_path)
    location_daily_linker.link_files()


if __name__ == "__main__":
    main()
