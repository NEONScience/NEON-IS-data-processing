#!/usr/bin/env python3
from pathlib import Path
from structlog import get_logger

import environs
import math
import os
import pandas as pd

from flow_sae_trst_dp0p.cal_flags import get_cal_val_flags
from flow_sae_trst_dp0p import log_config

log = get_logger()

cvdry = 717.6
cpdry = 1004.64
mdry = 28.9645
r = 8314.4621
term_map = {'ux_wind_speed': 'veloXaxs', 'uy_wind_speed': 'veloYaxs',
            'uz_wind_speed': 'veloZaxs', 'speed_of_sound': 'veloSoni'}
data_columns = ['time', 'veloXaxs', 'veloYaxs', 'veloZaxs', 'veloSoni', 'tempSoni', 'idx',
                        'qfSoniUnrs', 'qfSoniData', 'qfSoniTrig', 'qfSoniComm', 'qfSoniCode',
                        'qfSoniTemp', 'qfSoniSgnlPoor', 'qfSoniSgnlHigh', 'qfSoniSgnlLow']


def data_conversion(filename: str) -> pd.DataFrame:
    df = pd.read_parquet(filename)
    outputdf = df.copy()
    log.debug(f'{outputdf.columns}')
    outputdf['tempSoni'] = outputdf['speed_of_sound'].apply(lambda x: -1 if math.isnan(x) else get_temp_soni(x))
    outputdf['index'] = outputdf['diagnostic_word'].apply(lambda x: -1 if math.isnan(x) else get_range_bits(x, 0, 5))
    outputdf['qfSoniUnrs'] = outputdf['diagnostic_word'].apply(lambda x: -1 if math.isnan(x) else 1 if x == -99999 else 0)
    outputdf['qfSoniData'] = outputdf['diagnostic_word'].apply(lambda x: -1 if math.isnan(x) else 1 if x == 61503 else 0)
    outputdf['qfSoniTrig'] = outputdf['diagnostic_word'].apply(lambda x: -1 if math.isnan(x) else 1 if x == 61440 else 0)
    outputdf['qfSoniComm'] = outputdf['diagnostic_word'].apply(lambda x: -1 if math.isnan(x) else 1 if x == 61441 else 0)
    outputdf['qfSoniCode'] = outputdf['diagnostic_word'].apply(lambda x: -1 if math.isnan(x) else 1 if x == 61442 else 0)
    outputdf['qfSoniTemp'] = outputdf['diagnostic_word'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit(x, 15))
    outputdf['qfSoniSgnlPoor'] = outputdf['diagnostic_word'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit(x, 14))
    outputdf['qfSoniSgnlHigh'] = outputdf['diagnostic_word'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit(x, 13))
    outputdf['qfSoniSgnlLow'] = outputdf['diagnostic_word'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit(x, 12))
    del outputdf['source_id']
    del outputdf['site_id']
    del outputdf['diagnostic_word']
    outputdf.columns = data_columns
    return outputdf


def get_temp_soni(veloSoni: float) -> float:
    return veloSoni * veloSoni * cvdry * mdry / (cpdry * r)


def get_nth_bit(number, n) -> int:
    return (int(number) >> n) & 0x0001


def get_range_bits(value, start, end) -> int:
    mask = ~(-1 << (end - start + 1)) << start
    return (int(value) & mask) >> start


def l0tol0p(in_path: Path, out_path: Path, file_dirs: list, relative_path_index: int) -> None:
    """
    L0 to l0p transformation.
    :param in_path: The input path for files.
    :param out_path: The output path for linking.
    :param file_dirs: The directories to files need l0 to l0p transformation.
    :param relative_path_index: Starting index of the input path to include in the output path.
    """

    outputdf = pd.DataFrame()
    outfile = ''
    for root, directories, files in os.walk(str(in_path)):
        if not outputdf.empty and len(directories) > 0:
            if any(dir in directories for dir in file_dirs):
                outputdf.to_parquet(outfile)
                outputdf = pd.DataFrame()
                outfile = ''
        if len(files) > 0:
            if len(files) > 1:
                log.warn("There are more than 1 files under " + root)
                log.warn(files)
            for file in files:
                path = Path(root, file)
                if "flag" in str(path):
                    if outputdf.empty:
                        outputdf = get_cal_val_flags(path, term_map)
                    else:
                        outputdf = pd.merge(outputdf, get_cal_val_flags(path, term_map), how='inner', left_on=['time'], right_on=['time'])
                    outputdf['qfCalTempSoni'] = outputdf['qfCalVeloSoni']
                else:
                    outfile = Path(out_path, *Path(path).parts[relative_path_index:])
                    outfile.parent.mkdir(parents=True, exist_ok=True)
                    if outputdf.empty:
                        outputdf = data_conversion(path)
                    else:
                        outputdf = pd.merge(data_conversion(path), outputdf, how='inner', left_on=['time'], right_on=['time'])


def main() -> None:
    env = environs.Env()
    in_path: Path = env.str('IN_PATH')
    out_path: Path = env.path('OUT_PATH')
    file_dirs: list = env.list('FILE_DIR')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    relative_path_index: int = env.int('RELATIVE_PATH_INDEX')
    log_config.configure(log_level)

    l0tol0p(in_path, out_path, file_dirs, relative_path_index)


if __name__ == "__main__":
    main()