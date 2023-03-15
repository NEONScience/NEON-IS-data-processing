#!/usr/bin/env python3
from structlog import get_logger

import math
import pandas as pd

from flow_sae_trst_dp0p.l0tol0p import L0toL0p
from flow_sae_trst_dp0p.shared_functions import get_nth_bit, get_range_bits

log = get_logger()


class Csat3(L0toL0p):

    mdry = 28.9645
    r = 8314.4621
    gammad = 1.4
    data_columns = {'ux_wind_speed': 'veloXaxs',
                    'uy_wind_speed': 'veloYaxs',
                    'uz_wind_speed': 'veloZaxs',
                    'speed_of_sound': 'veloSoni'}

    def data_conversion(self, filename: str) -> pd.DataFrame:
        df = super().data_conversion(filename)

        df['tempSoni'] = df['speed_of_sound'].apply(lambda x: math.nan if math.isnan(x) else self.get_temp_soni(x)).astype('float32')
        df['idx'] = df['diagnostic_word'].apply(lambda x: math.nan if math.isnan(x) else get_range_bits(x, 0, 5)).astype('float32')
        df['qfSoniUnrs'] = df['diagnostic_word'].apply(lambda x: -1 if math.isnan(x) else 1 if x == -99999 else 0).astype('int8')
        df['qfSoniData'] = df['diagnostic_word'].apply(lambda x: -1 if math.isnan(x) else 1 if x == 61503 else 0).astype('int8')
        df['qfSoniTrig'] = df['diagnostic_word'].apply(lambda x: -1 if math.isnan(x) else 1 if x == 61440 else 0).astype('int8')
        df['qfSoniComm'] = df['diagnostic_word'].apply(lambda x: -1 if math.isnan(x) else 1 if x == 61441 else 0).astype('int8')
        df['qfSoniCode'] = df['diagnostic_word'].apply(lambda x: -1 if math.isnan(x) else 1 if x == 61442 else 0).astype('int8')
        df['qfSoniTemp'] = df['diagnostic_word'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit(x, 15)).astype('int8')
        df['qfSoniSgnlPoor'] = df['diagnostic_word'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit(x, 14)).astype('int8')
        df['qfSoniSgnlHigh'] = df['diagnostic_word'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit(x, 13)).astype('int8')
        df['qfSoniSgnlLow'] = df['diagnostic_word'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit(x, 12)).astype('int8')
        del df['diagnostic_word']
        df.rename(columns=self.data_columns, inplace=True)
        log.debug(f'{df.columns}')
        return df

    def get_temp_soni(self, veloSoni: float) -> float:
        return veloSoni * veloSoni * self.mdry / (self.gammad * self.r)


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
