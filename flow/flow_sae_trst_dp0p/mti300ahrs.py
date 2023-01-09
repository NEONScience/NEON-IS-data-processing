#!/usr/bin/env python3
from structlog import get_logger

import math
import pandas as pd

from flow_sae_trst_dp0p.l0tol0p import L0toL0p
from flow_sae_trst_dp0p.shared_functions import get_nth_bit_opposite, get_nth_bit, get_degree_radian

log = get_logger()


class Mti300Ahrs(L0toL0p):

    data_columns = {'packet_counter': 'idx',
                    'acceleration_x': 'accXaxs',
                    'acceleration_y': 'accYaxs',
                    'acceleration_z': 'accZaxs',
                    'free_acceleration_x': 'accXaxsDiff',
                    'free_acceleration_y': 'accYaxsDiff',
                    'free_acceleration_z': 'accZaxsDiff',
                    'gyroscope_x': 'avelXaxs',
                    'gyroscope_y': 'avelYaxs',
                    'gyroscope_z': 'avelZaxs'}

    def data_conversion(self, filename) -> pd.DataFrame:
        df = super().data_conversion(filename)

        df['angXaxs'] = df['roll'].apply(lambda x: math.nan if math.isnan(x) else get_degree_radian(x)).astype('float32')
        del df['roll']
        df['angYaxs'] = df['pitch'].apply(lambda x: math.nan if math.isnan(x) else get_degree_radian(x)).astype('float32')
        del df['pitch']
        df['angZaxs'] = df['yaw'].apply(lambda x: math.nan if math.isnan(x) else get_degree_radian(x)).astype('float32')
        del df['yaw']

        df['qfAmrsVal'] = df['status_word'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit_opposite(x, 0)).astype('int8')
        df['qfAmrsFilt'] = df['status_word'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit_opposite(x, 1)).astype('int8')
        df['qfAmrsVelo'] = df['status_word'].apply(lambda x: -1 if math.isnan(x) else (get_nth_bit(x, 17) or get_nth_bit(x, 18))).astype('int8')
        df['qfAmrsRng'] = df['status_word'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit(x, 19)).astype('int8')
        del df['status_word']
        df.rename(columns=self.data_columns, inplace=True)
        log.debug(f'{df.columns}')

        return df


def main() -> None:

    mti300ahrs = Mti300Ahrs()
    mti300ahrs.l0tol0p()
    log.debug("done.")


if __name__ == "__main__":
    main()
