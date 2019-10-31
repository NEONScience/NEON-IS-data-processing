import environs
from structlog import get_logger

from lib import log_config as log_config
from timeseries_padder.timeseries_padder.padder import Padder


def main():
    env = environs.Env()
    data_path = env('DATA_PATH')
    out_path = env('OUT_PATH')
    log_level = env('LOG_LEVEL')
    log_config.configure(log_level)
    log = get_logger()
    log.debug(f'data_dir: {data_path}')
    log.debug(f'out_dir: {out_path}')
    padder = Padder(data_path, out_path)
    padder.pad()


if __name__ == '__main__':
    main()
