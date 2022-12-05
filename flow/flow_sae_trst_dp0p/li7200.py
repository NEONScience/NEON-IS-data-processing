#!/usr/bin/env python3
from structlog import get_logger

import math
import pandas as pd

from flow_sae_trst_dp0p.l0tol0p import L0toL0p
from flow_sae_trst_dp0p.shared_functions import get_nth_bit_opposite, get_range_bits, from_percentage
from flow_sae_trst_dp0p.shared_functions import get_temp_kelvin, get_pressure_pa, mmol_to_mol, umol_to_mol

log = get_logger()


class Li7200(L0toL0p):

    data_columns = {'h2o_absorbing_wavelength': 'powrH2oSamp',
                    'h2o_nonabsorbing_wavelength': 'powrH2oRefe',
                    'h2o_raw': 'asrpH2o',
                    'co2_raw': 'asrpCo2',
                    'index': 'idx',
                    'co2_absorbing_wavelength': 'powrCo2Samp',
                    'co2_nonabsorbing_wavelength': 'powrCo2Refe',
                    'diagnostic_value_2': 'diag02',
                    'cooler': 'potCool'}

    drop_columns = ['date', 'time', 'co2_mass_density', 'h2o_mass_density', 'auxiliary_input_1',
                    'auxiliary_input_2', 'auxiliary_input_3', 'auxiliary_input_4',
                    'smart_flux_voltage_in', 'co2_mole_fraction', 'h2o_mole_fraction',
                    'dew_point', 'average_temperature', 'average_signal_strength', 'delta_signal_strength',
                    'measured_flow_rate', 'volumetric_flow_rate', 'flow_pressure', 'flow_power', 'flow_drive']

    def data_conversion(self, filename) -> pd.DataFrame:
        df = super().data_conversion(filename)
        df.drop(columns=self.drop_columns, inplace=True)

        df['tempIn'] = df['temperature_inlet'].apply(lambda x: math.nan if math.isnan(x) else get_temp_kelvin(x)).astype('float32')
        del df['temperature_inlet']
        df['tempOut'] = df['temperature_outlet'].apply(lambda x: math.nan if math.isnan(x) else get_temp_kelvin(x)).astype('float32')
        del df['temperature_outlet']
        df['tempRefe'] = df['temperature'].apply(lambda x: math.nan if math.isnan(x) else get_temp_kelvin(x)).astype('float32')
        del df['temperature']
        df['tempMean'] = (df['tempIn'] + df['tempOut']) / 2
        df['presSum'] = df['pressure'].apply(lambda x: math.nan if math.isnan(x) else get_pressure_pa(x)).astype('float32')
        del df['pressure']
        df['presDiff'] = df['differential_pressure'].apply(lambda x: math.nan if math.isnan(x) else get_pressure_pa(x)).astype('float32')
        del df['differential_pressure']
        df['presAtm'] = df['absolute_pressure'].apply(lambda x: math.nan if math.isnan(x) else get_pressure_pa(x)).astype('float32')
        del df['absolute_pressure']
        # data from presto don't have presAtm, need to get from presSum-presDiff
        if df['presAtm'].isnull().all():
            df['presAtm'] = df['presSum'] - df['presDiff']

        df['densMoleH2o'] = df['h2o_concentration_density'].apply(lambda x: math.nan if math.isnan(x) else mmol_to_mol(x)).astype('float32')
        del df['h2o_concentration_density']
        df['rtioMoleDryH2o'] = df['h2o_mole_fraction_dry'].apply(lambda x: math.nan if math.isnan(x) else mmol_to_mol(x)).astype('float32')
        del df['h2o_mole_fraction_dry']

        df['densMoleCo2'] = df['co2_molar_density'].apply(lambda x: math.nan if math.isnan(x) else mmol_to_mol(x)).astype('float32')
        del df['co2_molar_density']
        df['rtioMoleDryCo2'] = df['co2_mole_fraction_dry'].apply(lambda x: math.nan if math.isnan(x) else umol_to_mol(x)).astype('float32')
        del df['co2_mole_fraction_dry']

        df['ssiCo2'] = df['co2_signal_strength'].apply(lambda x: math.nan if math.isnan(x) else from_percentage(x)).astype('float32')
        del df['co2_signal_strength']
        df['ssiH2o'] = df['h2o_signal_strength'].apply(lambda x: math.nan if math.isnan(x) else from_percentage(x)).astype('float32')
        del df['h2o_signal_strength']

        df['qfIrgaTurbHead'] = df['diagnostic_value'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit_opposite(x, 12)).astype('int8')
        df['qfIrgaTurbTempOut'] = df['diagnostic_value'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit_opposite(x, 11)).astype('int8')
        df['qfIrgaTurbTempIn'] = df['diagnostic_value'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit_opposite(x, 10)).astype('int8')
        df['qfIrgaTurbAux'] = df['diagnostic_value'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit_opposite(x, 9)).astype('int8')
        df['qfIrgaTurbPres'] = df['diagnostic_value'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit_opposite(x, 8)).astype('int8')
        df['qfIrgaTurbChop'] = df['diagnostic_value'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit_opposite(x, 7)).astype('int8')
        df['qfIrgaTurbDetc'] = df['diagnostic_value'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit_opposite(x, 6)).astype('int8')
        df['qfIrgaTurbPll'] = df['diagnostic_value'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit_opposite(x, 5)).astype('int8')
        df['qfIrgaTurbSync'] = df['diagnostic_value'].apply(lambda x: -1 if math.isnan(x) else get_nth_bit_opposite(x, 4)).astype('int8')
        lower4bits = df['diagnostic_value'].apply(lambda x: math.nan if math.isnan(x) else get_range_bits(x, 0, 3))
        df['qfIrgaTurbAgc'] = ((lower4bits * 6.25 + 6.25) / 100).astype('float32')
        del df['diagnostic_value']

        df.rename(columns=self.data_columns, inplace=True)
        log.debug(f'{df.columns}')
        return df


def main() -> None:

    # terms with calibration
    cal_term_map = {'h2o_raw': 'asrpH2o', 'co2_raw': 'asrpCo2'}

    li7200 = Li7200(cal_term_map)
    li7200.l0tol0p()

    log.debug("done.")


if __name__ == "__main__":
    main()
