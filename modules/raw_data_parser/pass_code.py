#!usr/bin/env python3

# li7200: irga, g2131i: crdCo2, l2130i: crdH2o
pass_code_dict = {
    'li7200_raw': {
        'Ndx': 'index',
        'DiagVal': 'diagnostic_value',
        'DiagVal2': 'diagnostic_value_2',
        'CO2Raw': 'co2_raw',
        'H2ORaw': 'h2o_raw',
        'CO2D': 'co2_molar_density',
        'H2OD': 'h2o_concentration_density',
        'Temp': 'temperature',
        'Pres': 'pressure',
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
        'presCavi': 1,
        'tempCavi': 2,
        'tempWarmBox': 5,
        'specID': 9,
        'fwMoleCO2': 10,
        'fdMoleCO2': 11,
        'fwMole12CO2': 12,
        'fdMole12CO2': 13,
        'fwMole13CO2': 14,
        'fdMole13CO2': 15,
        'd13CO2': 19,
        'percentFwMoleH2O': 20,
        'fwMoleHPCH4': 27,
        'fdMoleHPCH4': 28
    },
    'l2130i_raw': {
        'presCavi': 1,
        'tempCavi': 2,
        'tempWarmBox': 3,
        'valvMask': 8,
        'ppmvFwMoleH2O': 9,
        'd18OWater': 10,
        'd2HWater': 11,
        'N2Flag': 13
    }
}
