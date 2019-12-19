import environs
import argparse
from structlog import get_logger

from lib import log_config as log_config
from timeseries_padder.timeseries_padder.padder import Padder


def main():
    env = environs.Env()
    data_path = env('DATA_PATH')
    out_path = env('OUT_PATH')
    log_level = env('LOG_LEVEL')
    year_index = int(env('YEAR_INDEX'))
    month_index = int(env('MONTH_INDEX'))
    day_index = int(env('DAY_INDEX'))
    loc_index = int(env('LOC_INDEX'))
    subdir_index = int(env('SUBDIR_INDEX'))
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

    padder = Padder(data_path, out_path, year_index, month_index, day_index, loc_index, subdir_index)
    padder.pad()


if __name__ == '__main__':
    main()
