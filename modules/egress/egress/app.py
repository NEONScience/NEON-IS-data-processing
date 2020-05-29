#!/usr/bin/env python3
import environs
import argparse
from structlog import get_logger

from lib import log_config as log_config
from egress.egress.egress import Egress


def main():
    env = environs.Env()
    dataPath = env.path('DATA_PATH')
    outPath = env.path('OUT_PATH')
    logLevel = env.log_level('LOG_LEVEL')
    log_config.configure(logLevel)
    log = get_logger()
    log.debug(f'data_dir: {dataPath}')
    log.debug(f'out_dir: {outPath}')

    parser = argparse.ArgumentParser()
    parser.add_argument('--outputname')
    parser.add_argument('--dateindex')
    parser.add_argument('--locindex')
    args = parser.parse_args()

    egress = Egress(dataPath, outPath, args.outputname, int(args.dateindex), int(args.locindex))
    egress.upload()


if __name__ == '__main__':
    main()
