#!/usr/bin/env python3
import environs
import argparse
from structlog import get_logger

from common import log_config as log_config
from timeseries_padder.timeseries_padder.data_file_path import DataFilePath
from timeseries_padder.timeseries_padder.variable_window_pad import VariableWindowPad


def main():
    env = environs.Env()
    data_path = env.path('DATA_PATH')
    out_path = env.path('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL')
    log_config.configure(log_level)
    log = get_logger()
    log.debug(f'data_dir: {data_path} out_dir: {out_path}')

    parser = argparse.ArgumentParser()
    parser.add_argument('--yearindex')
    parser.add_argument('--monthindex')
    parser.add_argument('--dayindex')
    parser.add_argument('--locindex')
    parser.add_argument('--subdirindex')

    args = parser.parse_args()
    year_index = int(args.yearindex)
    month_index = int(args.monthindex)
    day_index = int(args.dayindex)
    location_index = int(args.locationindex)
    data_type_index = int(args.subdirindex)

    data_file_path = DataFilePath(year_index=year_index,
                                  month_index=month_index,
                                  day_index=day_index,
                                  location_index=location_index,
                                  data_type_index=data_type_index)
    variable_window_pad = VariableWindowPad(data_path=data_path,
                                            out_path=out_path,
                                            data_file_path=data_file_path)
    variable_window_pad.pad()


if __name__ == '__main__':
    main()
