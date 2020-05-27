#!/usr/bin/env python3
import environs
import structlog

import lib.log_config as log_config
from data_calibration_group.data_grouper import DataGrouper
from data_calibration_group.calibration_grouper import CalibrationGrouper


def group(data_grouper, calibration_grouper):
    for source in data_grouper.link_data():
        output_path = source.get('output_path')
        source_id = source.get('source_id')
        calibration_grouper.link_calibrations(source_id, output_path)


def main():
    env = environs.Env()
    data_path = env.path('DATA_PATH')
    calibration_path = env.path('CALIBRATION_PATH')
    out_path = env.path('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL')
    data_source_type_index = env.int('DATA_SOURCE_TYPE_INDEX')
    data_year_index = env.int('DATA_YEAR_INDEX')
    data_month_index = env.int('DATA_MONTH_INDEX')
    data_day_index = env.int('DATA_DAY_INDEX')
    calibration_source_type_index = env.int('CALIBRATION_SOURCE_TYPE_INDEX')
    calibration_source_id_index = env.int('CALIBRATION_SOURCE_ID_INDEX')
    calibration_stream_index = env.int('CALIBRATION_STREAM_INDEX')

    log_config.configure(log_level)
    log = structlog.get_logger()
    log.debug(f'data_path: {data_path}')
    log.debug(f'calibration_path: {calibration_path}')
    log.debug(f'out_path: {out_path}')

    data_grouper = DataGrouper(data_path,
                               out_path,
                               data_source_type_index,
                               data_year_index,
                               data_month_index,
                               data_day_index)
    calibration_grouper = CalibrationGrouper(calibration_path,
                                             calibration_source_type_index,
                                             calibration_source_id_index,
                                             calibration_stream_index)
    group(data_grouper, calibration_grouper)


if __name__ == '__main__':
    main()
