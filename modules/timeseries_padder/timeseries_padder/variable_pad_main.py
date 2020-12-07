#!/usr/bin/env python3
import environs
import argparse
from structlog import get_logger
from pathlib import Path

from common import log_config as log_config
from timeseries_padder.timeseries_padder.timeseries_padder_config import Config
from timeseries_padder.timeseries_padder.variable_pad import VariablePad


def main() -> None:
    env = environs.Env()
    data_path: Path = env.path('DATA_PATH')
    out_path: Path = env.path('OUT_PATH')
    log_level: str = env.log_level('LOG_LEVEL')
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
    location_index = int(args.locindex)
    data_type_index = int(args.subdirindex)
    config = Config(data_path=data_path,
                    out_path=out_path,
                    year_index=year_index,
                    month_index=month_index,
                    day_index=day_index,
                    location_index=location_index,
                    data_type_index=data_type_index,
                    relative_path_index=0,
                    window_size=0)
    variable_pad = VariablePad(config)
    variable_pad.pad()


if __name__ == '__main__':
    main()
