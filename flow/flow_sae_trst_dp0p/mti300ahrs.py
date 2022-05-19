#!/usr/bin/env python3
from structlog import get_logger

import math
import pandas as pd

from flow_sae_trst_dp0p.l0tol0p import L0toL0p
from flow_sae_trst_dp0p.shared_functions import get_nth_bit_opposite, get_nth_bit, get_degree_radian

log = get_logger()


class Mti300Ahrs(L0toL0p):

    data_columns = ['source_id', 'site', 'readout_time', 'idx', 'accXaxs', 'accYaxs', 'accZaxs', 'accXaxsDiff',
                    'accYaxsDiff', 'accZaxsDiff', 'avelXaxs', 'avelYaxs', 'avelZaxs']

    def data_conversion(self, filename) -> pd.DataFrame:
        df = pd.read_parquet(filename)
        outputdf = df.copy()
        log.debug(f'{df.columns}')
        log.info(df['site_id'][1])
        log.info(df['source_id'][1])
        # del outputdf['source_id']
        # del outputdf['site_id']
        del outputdf['roll']
        del outputdf['pitch']
        del outputdf['yaw']
        del outputdf['status_word']
        outputdf.columns = self.data_columns

        outputdf['angXaxs'] = df['roll'].apply(lambda x: math.nan if math.isnan(x) else get_degree_radian(x))
        outputdf['angYaxs'] = df['pitch'].apply(lambda x: math.nan if math.isnan(x) else get_degree_radian(x))
        outputdf['angZaxs'] = df['yaw'].apply(lambda x: math.nan if math.isnan(x) else get_degree_radian(x))

        outputdf['qfAmrsVal'] = df['status_word'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit_opposite(x, 0))
        outputdf['qfAmrsFilt'] = df['status_word'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit_opposite(x, 1))
        outputdf['qfAmrsVelo'] = df['status_word'].apply(lambda x: -1 if math.isnan(x) else (get_nth_bit(x, 17) or get_nth_bit(x, 18)))
        outputdf['qfAmrsRng'] = df['status_word'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit(x, 19))
        return outputdf


def main() -> None:

    mti300ahrs = Mti300Ahrs()
    mti300ahrs.l0tol0p()


if __name__ == "__main__":
    main()
