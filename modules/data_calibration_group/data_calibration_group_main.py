#!/usr/bin/env python3
import environs
import structlog
from pathlib import Path

import common.log_config as log_config

from data_calibration_group.data_calibration_group_config import Config
from data_calibration_group.data_calibration_grouper import group_files


def main() -> None:
    env = environs.Env()
    data_path: Path = env.path('DATA_PATH')
    calibration_path: Path = env.path('CALIBRATION_PATH')
    out_path: Path = env.path('OUT_PATH')
    log_level: str = env.log_level('LOG_LEVEL')
    data_source_id_index: int = env.int('DATA_SOURCE_ID_INDEX')
    data_source_type_index: int = env.int('DATA_SOURCE_TYPE_INDEX')
    data_year_index: int = env.int('DATA_YEAR_INDEX')
    data_month_index: int = env.int('DATA_MONTH_INDEX')
    data_day_index: int = env.int('DATA_DAY_INDEX')
    calibration_source_type_index: int = env.int('CALIBRATION_SOURCE_TYPE_INDEX')
    calibration_source_id_index: int = env.int('CALIBRATION_SOURCE_ID_INDEX')
    calibration_stream_index: int = env.int('CALIBRATION_STREAM_INDEX')

    log_config.configure(log_level)
    log = structlog.get_logger()
    log.debug(f'data_path: {data_path} calibration_path: {calibration_path} out_path: {out_path}')

    config = Config(data_path=data_path,
                    calibration_path=calibration_path,
                    out_path=out_path,
                    data_source_type_index=data_source_type_index,
                    data_source_id_index=data_source_id_index,
                    data_year_index=data_year_index,
                    data_month_index=data_month_index,
                    data_day_index=data_day_index,
                    calibration_source_type_index=calibration_source_type_index,
                    calibration_source_id_index=calibration_source_id_index,
                    calibration_stream_index=calibration_stream_index)
    group_files(config)


if __name__ == '__main__':
    main()
