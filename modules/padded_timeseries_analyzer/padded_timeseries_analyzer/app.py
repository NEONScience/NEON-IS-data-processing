#!/usr/bin/env python3
import environs
from structlog import get_logger

from lib import log_config as log_config

import padded_timeseries_analyzer.padded_timeseries_analyzer.analyzer as analyzer


def main():
    """Analyze padded time series data"""
    env = environs.Env()
    data_path = env.str('DATA_PATH')
    out_path = env.str('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL')
    relative_path_index = env.int('RELATIVE_PATH_INDEX')
    log_config.configure(log_level)
    log = get_logger()
    log.debug(f'data_path: {data_path}')
    log.debug(f'out_path: {out_path}')
    analyzer.analyze(data_path, out_path, relative_path_index)


if __name__ == '__main__':
    main()
