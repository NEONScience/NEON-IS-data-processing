#!/usr/bin/env python3
from structlog import get_logger

import math
import pandas as pd

from flow_sae_trst_dp0p.l0tol0p import L0toL0p

log = get_logger()

class Pump(L0toL0p):
    def data_conversion(self, filename) -> pd.DataFrame:
        df = super().data_conversion(filename)
        df.rename(columns={'dac_output': 'pumpVoltage'}, inplace=True)
        log.debug(f'{df.columns}')
        return df


def main() -> None:

    context_dp_map = {'storage': 'pumpStor', 'turbulent': 'pumpTurb'}
    pump = Pump(context_dp_map=context_dp_map)
    pump.l0tol0p()
    log.debug("done.")


if __name__ == "__main__":
    main()
