#!/usr/bin/env python3
from structlog import get_logger

import math
import pandas as pd

from flow_sae_trst_dp0p.l0tol0p import L0toL0p
from flow_sae_trst_dp0p.shared_functions import get_nth_bit, get_range_bits

log = get_logger()


class Csat3(L0toL0p):

    cvdry = 717.6
    cpdry = 1004.64
    mdry = 28.9645
    r = 8314.4621
    data_columns = ['source_id', 'site', 'readout_time', 'veloXaxs', 'veloYaxs', 'veloZaxs', 'veloSoni']

    def data_conversion(self, filename: str) -> pd.DataFrame:
        df = pd.read_parquet(filename)
        outputdf = df.copy()
        log.debug(f'{outputdf.columns}')
        log.info(df['site_id'][0])
        log.info(df['source_id'][0])
        # del outputdf['source_id']
        # del outputdf['site_id']
        del outputdf['diagnostic_word']
        outputdf.columns = self.data_columns

        outputdf['tempSoni'] = df['speed_of_sound'].apply(lambda x: math.nan if math.isnan(x) else self.get_temp_soni(x))
        outputdf['index'] = df['diagnostic_word'].apply(lambda x: math.nan if math.isnan(x) else get_range_bits(x, 0, 5))
        outputdf['qfSoniUnrs'] = df['diagnostic_word'].apply(lambda x: -1 if math.isnan(x) else 1 if x == -99999 else 0)
        outputdf['qfSoniData'] = df['diagnostic_word'].apply(lambda x: -1 if math.isnan(x) else 1 if x == 61503 else 0)
        outputdf['qfSoniTrig'] = df['diagnostic_word'].apply(lambda x: -1 if math.isnan(x) else 1 if x == 61440 else 0)
        outputdf['qfSoniComm'] = df['diagnostic_word'].apply(lambda x: -1 if math.isnan(x) else 1 if x == 61441 else 0)
        outputdf['qfSoniCode'] = df['diagnostic_word'].apply(lambda x: -1 if math.isnan(x) else 1 if x == 61442 else 0)
        outputdf['qfSoniTemp'] = df['diagnostic_word'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit(x, 15))
        outputdf['qfSoniSgnlPoor'] = df['diagnostic_word'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit(x, 14))
        outputdf['qfSoniSgnlHigh'] = df['diagnostic_word'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit(x, 13))
        outputdf['qfSoniSgnlLow'] = df['diagnostic_word'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit(x, 12))
        return outputdf

    def get_temp_soni(self, veloSoni: float) -> float:
        return veloSoni * veloSoni * self.cvdry * self.mdry / (self.cpdry * self.r)


def main() -> None:

    # terms with calibration
    cal_term_map = {'ux_wind_speed': 'veloXaxs', 'uy_wind_speed': 'veloYaxs',
                    'uz_wind_speed': 'veloZaxs', 'speed_of_sound': 'veloSoni'}
    # pass along qf cal
    calibrated_qf_list = ['qfCalVeloSoni']
    target_qf_cal_list = ['qfCalTempSoni']

    csat3 = Csat3(cal_term_map, calibrated_qf_list, target_qf_cal_list)
    csat3.l0tol0p()


if __name__ == "__main__":
    main()
