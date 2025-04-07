#!usr/bin/env python3

# li7200: irga, g2131i: crdCo2, l2130i: crdH2o
parse_code_dict = {
    'li7200_raw': {
        'Ndx': 'index',
        'DiagVal': 'diagnostic_value',
        'DiagVal2': 'diagnostic_value_2',
        'CO2Raw': 'co2_raw',
        'H2ORaw': 'h2o_raw',
        'CO2D': 'co2_molar_density',
        'H2OD': 'h2o_molar_density',
        'Temp': 'temperature',
        'Pres': 'pressure',
        'DPres': 'differential_pressure',
        'Cooler': 'cooler',
        'CO2MFd': 'co2_mole_fraction_dry',
        'H2OMFd': 'h2o_mole_fraction_dry',
        'TempIn': 'temperature_inlet',
        'TempOut': 'temperature_outlet',
        'CO2SS': 'co2_signal_strength',
        'H2OSS': 'h2o_signal_strength',
        'H2OAW': 'h2o_absorbing_wavelength',
        'H2OAWO': 'h2o_nonabsorbing_wavelength',
        'CO2AW': 'co2_absorbing_wavelength',
        'CO2AWO': 'co2_nonabsorbing_wavelength'
    },
    'g2131i_raw': {
        1: 'presCavi',
        2: 'tempCavi',
        5: 'tempWarmBox',
        9: 'specID',
        10: 'fwMoleCO2',
        11: 'fdMoleCO2',
        12: 'fwMole12CO2',
        13: 'fdMole12CO2',
        14: 'fwMole13CO2',
        15: 'fdMole13CO2',
        19: 'd13CO2',
        20: 'percentFwMoleH2O',
        27: 'fwMoleHPCH4',
        28: 'fdMoleHPCH4'
    },
    'l2130i_raw': {
        1: 'presCavi',
        2: 'tempCavi',
        3: 'tempWarmBox',
        8: 'valvMask',
        9: 'ppmvFwMoleH2O',
        10: 'd18OWater',
        11: 'd2HWater',
        13: 'N2Flag'
    }
}
