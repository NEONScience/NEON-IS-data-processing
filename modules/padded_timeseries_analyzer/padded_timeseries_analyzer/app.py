import environs
from structlog import get_logger

from lib import log_config as log_config
import padded_timeseries_analyzer.padded_timeseries_analyzer.analyzer as analyzer


def main():
    """Analyze padded time series data"""
    env = environs.Env()
    data_path = env('DATA_PATH')
    out_path = env('OUT_PATH')
    log_level = env('LOG_LEVEL')
    log_config.configure(log_level)
    log = get_logger()
    log.debug(f'data_path: {data_path}')
    log.debug(f'out_path: {out_path}')
    analyzer.analyze(data_path, out_path)


if __name__ == '__main__':
    main()
