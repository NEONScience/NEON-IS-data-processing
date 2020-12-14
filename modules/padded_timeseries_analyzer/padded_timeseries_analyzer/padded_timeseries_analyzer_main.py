#!/usr/bin/env python3
import environs
from structlog import get_logger
from pathlib import Path

from common import log_config as log_config

from padded_timeseries_analyzer.padded_timeseries_analyzer.padded_timeseries_analyzer import PaddedTimeSeriesAnalyzer


def main() -> None:
    """Analyze padded time series data"""
    env = environs.Env()
    data_path: Path = env.path('DATA_PATH')
    out_path: Path = env.path('OUT_PATH')
    log_level: str = env.log_level('LOG_LEVEL')
    relative_path_index: int = env.int('RELATIVE_PATH_INDEX')
    log_config.configure(log_level)
    log = get_logger()
    log.debug(f'data_path: {data_path} out_path: {out_path}')
    analyzer = PaddedTimeSeriesAnalyzer(data_path, out_path, relative_path_index)
    analyzer.analyze()


if __name__ == '__main__':
    main()
