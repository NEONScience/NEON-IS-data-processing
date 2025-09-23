#!/usr/bin/env python3
from structlog import get_logger

import math
import pandas as pd

from flow_sae_trst_dp0p.l0tol0p import L0toL0p
from flow_sae_trst_dp0p.shared_functions import get_nth_bit, get_range_bits
from flow_sae_trst_dp0p.shared_functions import get_nth_bit_opposite, get_range_bits, from_percentage
from flow_sae_trst_dp0p.shared_functions import get_temp_kelvin, get_pressure_pa, mmol_to_mol, umol_to_mol


log = get_logger()

class Li840a(L0toL0p):


    def data_conversion(self, filename: str) -> pd.DataFrame:
        df = super().data_conversion(filename)

        df['rtioMoleWetCo2'] = df['fwMoleCO2'].apply(lambda x: math.nan if math.isnan(x) else umol_to_mol(x)).astype('float32')
        del df['fwMoleCO2']
        df['rtioMoleWetH2o'] = df['fwMoleH2O'].apply(lambda x: math.nan if math.isnan(x) else mmol_to_mol(x)).astype('float32')
        del df['fwMoleH2O']
        df['temp'] = df['tempCell'].apply(lambda x: math.nan if math.isnan(x) else get_temp_kelvin(x)).astype('float32')
        del df['tempCell']
        df['pres'] = df['presCell'].apply(lambda x: math.nan if math.isnan(x) else get_pressure_pa(x)).astype('float32')
        del df['presCell']
        df['asrpCo2'] = df['asrpCO2']
        df['asrpH2o'] = df['asrpH2O']
        df['rtioMoleDryCo2'] = df['rtioMoleWetCo2']/(1-rtioMoleWetH2o)
        df['rtioMoleDryH2o'] = df['rtioMoleWetH2o']/(1-rtioMoleWetH2o)
        
        df.rename(columns=self.data_columns, inplace=True)
        log.debug(f'{df.columns}')
        return df


def main() -> None:

    # terms with calibration
    li840a = Li840a()
    li840a.l0tol0p()
    li840a.l0tol0p()

    log.debug("done.")


if __name__ == "__main__":
    main()
