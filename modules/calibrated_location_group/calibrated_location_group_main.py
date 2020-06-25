#!/usr/bin/env python3
import environs
import structlog
from pathlib import Path

import common.log_config as log_config
from calibrated_location_group.calibrated_file_path import CalibratedFilePath
from calibrated_location_group.calibrated_location_file_grouper import CalibratedLocationFileGrouper

log = structlog.get_logger()


def main():
    env = environs.Env()
    calibrated_path: Path = env.path('CALIBRATED_PATH')
    location_path: Path = env.path('LOCATION_PATH')
    out_path: Path = env.path('OUT_PATH')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    source_type_index: int = env.int('SOURCE_TYPE_INDEX')
    year_index: int = env.int('YEAR_INDEX')
    month_index: int = env.int('MONTH_INDEX')
    day_index: int = env.int('DAY_INDEX')
    source_id_index: int = env.int('SOURCE_ID_INDEX')
    data_type_index: int = env.int('DATA_TYPE_INDEX')
    log_config.configure(log_level)
    log.debug(f'calibrated_path: {calibrated_path} location_path: {location_path} out_path: {out_path}')
    calibrated_file_path = CalibratedFilePath(source_type_index=source_type_index,
                                              year_index=year_index,
                                              month_index=month_index,
                                              day_index=day_index,
                                              source_id_index=source_id_index,
                                              data_type_index=data_type_index)
    grouper = CalibratedLocationFileGrouper(calibrated_path=calibrated_path,
                                            location_path=location_path,
                                            out_path=out_path,
                                            calibrated_file_path=calibrated_file_path)
    grouper.group_files()


if __name__ == '__main__':
    main()
