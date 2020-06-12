#!/usr/bin/env python3
import environs
from structlog import get_logger

import common.log_config as log_config
from timeseries_padder.timeseries_padder.data_file_path import DataFilePath
from timeseries_padder.timeseries_padder.constant_window_pad import ConstantWindowPad

log = get_logger()


def main():
    env = environs.Env()
    data_path = env.path('DATA_PATH')
    out_path = env.path('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL')
    window_size = env.int('WINDOW_SIZE')
    year_index = env.int('YEAR_INDEX')
    month_index = env.int('MONTH_INDEX')
    day_index = env.int('DAY_INDEX')
    location_index = env.int('LOCATION_INDEX')
    data_type_index = env.int('DATA_TYPE_INDEX')
    relative_path_index = env.int('RELATIVE_PATH_INDEX')
    log_config.configure(log_level)
    data_file_path = DataFilePath(year_index=year_index,
                                  month_index=month_index,
                                  day_index=day_index,
                                  location_index=location_index,
                                  data_type_index=data_type_index)
    constant_window_pad = ConstantWindowPad(data_path=data_path,
                                            out_path=out_path,
                                            relative_path_index=relative_path_index,
                                            window_size=window_size,
                                            data_file_path=data_file_path)
    constant_window_pad.pad()


if __name__ == '__main__':
    main()
