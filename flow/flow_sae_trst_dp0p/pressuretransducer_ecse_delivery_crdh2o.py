#!/usr/bin/env python3
import math
import pandas as pd
from flow_sae_trst_dp0p.pressuretransducer import PressureTransducer


class PressureTransducerCrdH2o(PressureTransducer):

    """
    specific class for ECSE delivery pressure from zero to crd H2O which includes pressure flag
    """
    press_low = 13790
    press_high = 20684

    def data_conversion(self, filename) -> pd.DataFrame:
        outputdf = super.data_conversion(filename)
        outputdf['qfPresDiff'] = outputdf['presDiff'].apply(lambda x: -1 if math.isnan(x) else self.get_qf_pressure(x))

        return outputdf

    def get_qf_pressure(self, pressure: float) -> int:
        if pressure < self.press_low or pressure > self.press_high:
            return 1
        else:
            return 0


def main() -> None:

    """
    output terms include presDiff and qfPresDiff, applicable for data products
    of ECSE: presValiRegOutStor (delivery pressure 00110, only for pressure from zero to crd h2o: HOR-711)
    """
    data_columns = {'pressure': 'presDiff'}
    presTrans = PressureTransducerCrdH2o(data_columns)
    presTrans.l0tol0p()


if __name__ == "__main__":
    main()
