#!/usr/bin/env python3
from structlog import get_logger

import math
import pandas as pd

from flow_sae_trst_dp0p.l0tol0p import L0toL0p
from flow_sae_trst_dp0p.shared_functions import get_temp_kelvin, get_pressure_pa, get_multiply_by

log = get_logger()


class Mcseries(L0toL0p):

    CONV_CONST: float = 1.666667 * 1E-5

    def data_conversion(self, filename) -> pd.DataFrame:
        df = super().data_conversion(filename)
        df['presAtm'] = df['absolute_pressure'].apply(lambda x: math.nan if math.isnan(x) else get_pressure_pa(x)).astype('float32')
        del df['absolute_pressure']
        df['temp'] = df['temperature'].apply(lambda x: math.nan if math.isnan(x) else get_temp_kelvin(x)).astype('float32')
        del df['temperature']
        df['frt'] = df['volumetric_flow'].apply(lambda x: math.nan if math.isnan(x) else get_multiply_by(x, self.CONV_CONST)).astype('float32')
        del df['volumetric_flow']
        df['frt00'] = df['mass_flow'].apply(lambda x: math.nan if math.isnan(x) else get_multiply_by(x, self.CONV_CONST)).astype('float32')
        del df['mass_flow']
        if 'setpoint' in df.columns:
            df['frtSet00'] = df['setpoint'].apply(lambda x: math.nan if math.isnan(x) else get_multiply_by(x, self.CONV_CONST)).astype('float32')
            del df['setpoint']

        # if self.new_source_type_name in ['mfcSampTurb', 'mfcValiTurb']:
        #     default as above
        if self.new_source_type_name == 'mfcSampStor':
            df['qfFrt00'] = df['frt00'].apply(lambda x: -1 if math.isnan(x) else self.get_qf_frt00_sampstor(x)).astype('int8')
        elif self.new_source_type_name == 'mfcValiStor':
            pass
            # TODO mfcValiStor need implementation
            # df['qfFrt00']

        log.debug(f'{df.columns}')
        return df

    def get_qf_frt00_sampstor(self, frt00: float) -> int:
        if frt00 < 0.8 * self.CONV_CONST or frt00 > 1.2 * self.CONV_CONST:
            return 1
        else:
            return 0


def main() -> None:

    context_dp_map = {'sampling,turbulent': 'mfcSampTurb', 'sampling,storage': 'mfcSampStor',
                      'turbulent,validation': 'mfcValiTurb', 'storage,validation': 'mfcValiStor'}

    mcseries = Mcseries(context_dp_map=context_dp_map)
    mcseries.l0tol0p()


if __name__ == "__main__":
    main()
