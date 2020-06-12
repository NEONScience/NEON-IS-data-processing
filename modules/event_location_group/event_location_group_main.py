#!/usr/bin/env python3
import environs
import structlog

import common.log_config as log_config

from event_location_group.data_file_path import DataFilePath
from event_location_group.event_location_grouper import EventLocationGrouper

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
    source_id_index = env.int('SOURCE_ID_INDEX')
    log_config.configure(log_level)
    log.debug(f'data_dir: {data_path} location_dir: {location_path} out_dir: {out_path}')

    data_file_path = DataFilePath(source_type_index=source_type_index,
                                  year_index=year_index,
                                  month_index=month_index,
                                  day_index=day_index,
                                  source_id_index=source_id_index)
    event_location_grouper = EventLocationGrouper(data_path=data_path,
                                                  location_path=location_path,
                                                  out_path=out_path,
                                                  data_file_path=data_file_path)
    event_location_grouper.group_files()


if __name__ == '__main__':
    main()
