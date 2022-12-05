#!/usr/bin/env python3
import math
import pandas as pd

from flow_sae_trst_dp0p.l0tol0p import L0toL0p
from flow_sae_trst_dp0p.shared_functions import get_pressure_pa
from structlog import get_logger

log = get_logger()


class PressureTransducer(L0toL0p):

    """
    base class for pressure transducer sensors
    """
    def __init__(self, data_columns):
        self.data_columns = data_columns
        super().__init__()

    def data_conversion(self, filename) -> pd.DataFrame:
        df = super().data_conversion(filename)

        df['pressure'] = df['pressure'].apply(lambda x: math.nan if math.isnan(x) else get_pressure_pa(x))
        df.rename(columns=self.data_columns, inplace=True)
        log.debug(f'{df.columns}')

        return df


def main() -> None:

    """
    output term is presDiff, applicable for data products
    of ECTE: presValiRegInTurb 00034, presValiRegOutTurb 00035, presValiLineTurb 00037
    and ECSE: presValiRegOutStor 00110 (delivery pressure, excludes pressure from zero to crd h2o: HOR-711)
    """
    data_columns = {'pressure': 'presDiff'}
    presTrans = PressureTransducer(data_columns)
    presTrans.l0tol0p()


if __name__ == "__main__":
    main()
