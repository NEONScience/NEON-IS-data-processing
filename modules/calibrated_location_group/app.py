#!/usr/bin/env python3
import environs
import structlog

import common.log_config as log_config
from calibrated_location_group.calibrated_location_file_grouper import CalibratedLocationFileGrouper

log = structlog.get_logger()


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
    file_grouper = CalibratedLocationFileGrouper(calibrated_path=calibrated_path,
                                                 location_path=location_path,
                                                 out_path=out_path,
                                                 source_type_index=source_type_index,
                                                 year_index=year_index,
                                                 month_index=month_index,
                                                 day_index=day_index,
                                                 source_id_index=source_id_index,
                                                 data_type_index=data_type_index)
    file_grouper.group()


if __name__ == '__main__':
    main()
