#!/usr/bin/env python3
import environs
import structlog

import common.log_config

from data_location_group.data_location_grouper import DataLocationGrouper

log = structlog.get_logger()


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
    file_index = env.int('FILE_INDEX')
    log.debug(f'data_dir: {data_path} location_dir: {location_path} out_dir: {out_path}')

    common.log_config.configure(log_level)

    data_location_grouper = DataLocationGrouper(data_path=data_path, location_path=location_path, out_path=out_path,
                                                source_type_index=source_type_index,
                                                year_index=year_index,
                                                month_index=month_index,
                                                day_index=day_index,
                                                file_index=file_index)
    data_location_grouper.group_files()


if __name__ == '__main__':
    main()
