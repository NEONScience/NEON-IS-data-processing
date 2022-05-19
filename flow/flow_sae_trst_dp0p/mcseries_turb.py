#!/usr/bin/env python3
from structlog import get_logger

import math
import pandas as pd

from flow_sae_trst_dp0p.l0tol0p import L0toL0p
from flow_sae_trst_dp0p.shared_functions import get_temp_kelvin, get_pressure_pa, get_multiply_by

log = get_logger()


class McseriesTurb(L0toL0p):

    CONV_CONST: float = 1.666667 * 1E-5

    def data_conversion(self, filename) -> pd.DataFrame:
        df = pd.read_parquet(filename)
        outputdf = pd.DataFrame()
        log.debug(f'{df.columns}')
        log.info(df['site_id'][0])
        log.info(df['source_id'][0])

        outputdf['source_id'] = df['source_id']
        outputdf['site'] = df['site_id']
        outputdf['readout_time'] = df['readout_time']
        outputdf['presAtm'] = df['absolute_pressure'].apply(lambda x: math.nan if math.isnan(x) else get_pressure_pa(x))
        outputdf['temp'] = df['temperature'].apply(lambda x: math.nan if math.isnan(x) else get_temp_kelvin(x))
        outputdf['frt'] = df['volumetric_flow'].apply(lambda x: math.nan if math.isnan(x) else get_multiply_by(x, self.CONV_CONST))
        outputdf['frt00'] = df['mass_flow'].apply(lambda x: math.nan if math.isnan(x) else get_multiply_by(x, self.CONV_CONST))
        if 'setpoint' in df.columns:
            outputdf['frtSet00'] = df['setpoint'].apply(lambda x: math.nan if math.isnan(x) else get_multiply_by(x, self.CONV_CONST))

        return outputdf


def main() -> None:

    mcseries = McseriesTurb()
    mcseries.l0tol0p()


if __name__ == "__main__":
    main()
