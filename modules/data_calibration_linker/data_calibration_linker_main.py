#!/usr/bin/env python3
import environs
import structlog
from pathlib import Path

import common.log_config as log_config

from data_calibration_linker.data_calibration_config import Config
from data_calibration_linker.data_calibration_linker import process_files


def main() -> None:
    env = environs.Env()
    data_path: Path = env.path('DATA_PATH', None)
    calibration_path: Path = env.path('CALIBRATION_PATH', None)
    out_path: Path = env.path('OUT_PATH')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    data_source_id_index: int = env.int('DATA_SOURCE_ID_INDEX')
    data_source_type_index: int = env.int('DATA_SOURCE_TYPE_INDEX')
    data_year_index: int = env.int('DATA_YEAR_INDEX')
    data_month_index: int = env.int('DATA_MONTH_INDEX')
    data_day_index: int = env.int('DATA_DAY_INDEX')
    relative_path_index: int = env.int('RELATIVE_PATH_INDEX')

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
                    relative_path_index=relative_path_index)
    process_files(config)


if __name__ == '__main__':
    main()
