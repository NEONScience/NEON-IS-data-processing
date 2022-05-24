#!/usr/bin/env python3
from structlog import get_logger
from typing import Dict
import math
import pandas as pd

log = get_logger()


def get_cal_val_flags(filename: str, term_map: Dict) -> pd.DataFrame:
    df = pd.read_parquet(filename)
    outputdf = pd.DataFrame()
    outputdf['readout_time'] = df['readout_time']
    log.debug(f'{df.columns}')
    for key,value in term_map.items():
        qfExpi = key + "_qfExpi"
        qfSusp = key + '_qfSusp'
        qfname = 'qfCal' + value[0].upper() + value[1:]
        outputdf[qfname] = df[qfExpi]
        outputdf.loc[df[qfSusp] == -1, qfname] = -1
        outputdf[qfname] = outputdf[qfname].replace(math.nan, -1)
    #outputdf.rename(columns={'readout_time': 'time'}, inplace=True)
    return outputdf
