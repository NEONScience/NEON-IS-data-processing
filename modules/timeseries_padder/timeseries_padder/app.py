#!/usr/bin/env python3
import environs
import argparse
from structlog import get_logger

from lib import log_config as log_config
from timeseries_padder.timeseries_padder.padder import Padder


def main():
    env = environs.Env()
    data_path = env.str('DATA_PATH')
    out_path = env.str('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL')
    log_config.configure(log_level)
    log = get_logger()
    log.debug(f'data_dir: {data_path}')
    log.debug(f'out_dir: {out_path}')

    parser = argparse.ArgumentParser()
    parser.add_argument('--yearindex')
    parser.add_argument('--monthindex')
    parser.add_argument('--dayindex')
    parser.add_argument('--locindex')
    parser.add_argument('--subdirindex')

    args = parser.parse_args()

    padder = Padder(data_path, out_path, int(args.yearindex), int(args.monthindex),
                    int(args.dayindex), int(args.locindex), int(args.subdirindex))

    padder.pad()


if __name__ == '__main__':
    main()
