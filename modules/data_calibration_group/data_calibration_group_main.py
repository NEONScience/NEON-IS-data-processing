#!/usr/bin/env python3
import environs
import structlog
from pathlib import Path

import common.log_config as log_config

from data_calibration_group.data_file_path import DataFilePath
from data_calibration_group.calibration_file_path import CalibrationFilePath
from data_calibration_group.data_calibration_grouper import DataCalibrationGrouper


def main():
    env = environs.Env()
    data_path: Path = env.path('DATA_PATH')
    calibration_path: Path = env.path('CALIBRATION_PATH')
    out_path: Path = env.path('OUT_PATH')
    log_level: str = env.log_level('LOG_LEVEL')
    data_source_type_index: int = env.int('DATA_SOURCE_TYPE_INDEX')
    data_year_index: int = env.int('DATA_YEAR_INDEX')
    data_month_index: int = env.int('DATA_MONTH_INDEX')
    data_day_index: int = env.int('DATA_DAY_INDEX')
    calibration_source_type_index: int = env.int('CALIBRATION_SOURCE_TYPE_INDEX')
    calibration_source_id_index: int = env.int('CALIBRATION_SOURCE_ID_INDEX')
    calibration_stream_index: int = env.int('CALIBRATION_STREAM_INDEX')

    log_config.configure(log_level)
    log = structlog.get_logger()
    log.debug(f'data_path: {data_path}')
    log.debug(f'calibration_path: {calibration_path}')
    log.debug(f'out_path: {out_path}')

    data_file_path = DataFilePath(source_type_index=data_source_type_index,
                                  year_index=data_year_index,
                                  month_index=data_month_index,
                                  day_index=data_day_index)
    calibration_file_path = CalibrationFilePath(source_type_index=calibration_source_type_index,
                                                source_id_index=calibration_source_id_index,
                                                stream_index=calibration_stream_index)
    data_calibration_grouper = DataCalibrationGrouper(data_path=data_path,
                                                      calibration_path=calibration_path,
                                                      out_path=out_path,
                                                      data_file_path=data_file_path,
                                                      calibration_file_path=calibration_file_path)
    data_calibration_grouper.group_files()


if __name__ == '__main__':
    main()
