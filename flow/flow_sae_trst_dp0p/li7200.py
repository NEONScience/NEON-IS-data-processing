#!/usr/bin/env python3
from structlog import get_logger

import math
import pandas as pd

from flow_sae_trst_dp0p.l0tol0p import L0toL0p
from flow_sae_trst_dp0p.shared_functions import get_nth_bit_opposite, get_range_bits, from_percentage
from flow_sae_trst_dp0p.shared_functions import get_temp_kelvin, get_pressure_pa, mmol_to_mol, umol_to_mol

log = get_logger()


class Li7200(L0toL0p):

    def data_conversion(self, filename) -> pd.DataFrame:
        df = pd.read_parquet(filename)
        outputdf = pd.DataFrame()
        log.debug(f'{df.columns}')
        log.info(df['site_id'][0])
        log.info(df['source_id'][0])

        outputdf['source_id'] = df['source_id']
        outputdf['site'] = df['site_id']
        outputdf['readout_time'] = df['readout_time']
        outputdf['tempIn'] = df['temperature_inlet'].apply(lambda x: math.nan if math.isnan(x) else get_temp_kelvin(x))
        outputdf['tempOut'] = df['temperature_outlet'].apply(lambda x: math.nan if math.isnan(x) else get_temp_kelvin(x))
        outputdf['tempRefe'] = df['temperature'].apply(lambda x: math.nan if math.isnan(x) else get_temp_kelvin(x))
        outputdf['tempMean'] = (outputdf['tempIn'] + outputdf['tempOut']) / 2
        outputdf['presSum'] = df['pressure'].apply(lambda x: math.nan if math.isnan(x) else get_pressure_pa(x))
        outputdf['presDiff'] = df['differential_pressure'].apply(lambda x: math.nan if math.isnan(x) else get_pressure_pa(x))
        outputdf['presAtm'] = df['absolute_pressure'].apply(lambda x: math.nan if math.isnan(x) else get_pressure_pa(x))
        # data from presto don't have presAtm, need to get from presSum-presDiff
        if outputdf['presAtm'].isnull().all():
            outputdf['presAtm'] = outputdf['presSum'] - outputdf['presDiff']

        outputdf['powrH2oSamp'] = df['h2o_absorbing_wavelength']
        outputdf['powrH2oRefe'] = df['h2o_nonabsorbing_wavelength']
        outputdf['asrpH2o'] = df['h2o_raw']
        outputdf['densMoleH2o'] = df['h2o_concentration_density'].apply(lambda x: math.nan if math.isnan(x) else mmol_to_mol(x))
        outputdf['rtioMoleDryH2o'] = df['h2o_mole_fraction_dry'].apply(lambda x: math.nan if math.isnan(x) else mmol_to_mol(x))

        outputdf['powrCo2Samp'] = df['co2_absorbing_wavelength']
        outputdf['powrCo2Refe'] = df['co2_nonabsorbing_wavelength']
        outputdf['asrpCo2'] = df['co2_raw']
        outputdf['densMoleCo2'] = df['co2_molar_density'].apply(lambda x: math.nan if math.isnan(x) else mmol_to_mol(x))
        outputdf['rtioMoleDryCo2'] = df['co2_mole_fraction_dry'].apply(lambda x: math.nan if math.isnan(x) else umol_to_mol(x))

        outputdf['idx'] = df['index']
        outputdf['diag02'] = df['diagnostic_value_2']
        outputdf['potCool'] = df['cooler']
        outputdf['ssiCo2'] = df['co2_signal_strength'].apply(lambda x: math.nan if math.isnan(x) else from_percentage(x))
        outputdf['ssiH2o'] = df['h2o_signal_strength'].apply(lambda x: math.nan if math.isnan(x) else from_percentage(x))

        outputdf['qfIrgaTurbHead'] = df['diagnostic_value'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit_opposite(x, 12))
        outputdf['qfIrgaTurbTempOut'] = df['diagnostic_value'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit_opposite(x, 11))
        outputdf['qfIrgaTurbTempIn'] = df['diagnostic_value'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit_opposite(x, 10))
        outputdf['qfIrgaTurbAux'] = df['diagnostic_value'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit_opposite(x, 9))
        outputdf['qfIrgaTurbPres'] = df['diagnostic_value'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit_opposite(x, 8))
        outputdf['qfIrgaTurbChop'] = df['diagnostic_value'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit_opposite(x, 7))
        outputdf['qfIrgaTurbDetc'] = df['diagnostic_value'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit_opposite(x, 6))
        outputdf['qfIrgaTurbPll'] = df['diagnostic_value'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit_opposite(x, 5))
        outputdf['qfIrgaTurbSync'] = df['diagnostic_value'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit_opposite(x, 4))
        lower4bits = df['diagnostic_value'].apply(lambda x: math.nan if math.isnan(x) else get_range_bits(x, 0, 3))
        outputdf['qfIrgaTurbAgc'] = (lower4bits * 6.25 + 6.25) / 100
        return outputdf


def main() -> None:

    # terms with calibration
    cal_term_map = {'h2o_raw': 'asrpH2o', 'co2_raw': 'asrpCo2'}

    li7200 = Li7200(cal_term_map)
    li7200.l0tol0p()


if __name__ == "__main__":
    main()
