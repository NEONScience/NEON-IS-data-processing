#!/usr/bin/env python3
import environs
import structlog

import lib.log_config

from data_location_group.data_location_grouper import link_data, link_location

log = structlog.get_logger()


def main():
    env = environs.Env()
    data_path = env('DATA_PATH')
    location_path = env('LOCATION_PATH')
    out_path = env('OUT_PATH')
    log_level = env('LOG_LEVEL')
    source_type_index = env.int('SOURCE_TYPE_INDEX')
    year_index = env.int('YEAR_INDEX')
    month_index = env.int('MONTH_INDEX')
    day_index = env.int('DAY_INDEX')
    file_index = env.int('FILE_INDEX')
    log.debug(f'data_dir: {data_path} location_dir: {location_path} out_dir: {out_path}')

    lib.log_config.configure(log_level)

    for output_dir in link_data(data_path,
                                out_path,
                                source_type_index,
                                year_index,
                                month_index,
                                day_index,
                                file_index):
        link_location(location_path, output_dir)


if __name__ == '__main__':
    main()
