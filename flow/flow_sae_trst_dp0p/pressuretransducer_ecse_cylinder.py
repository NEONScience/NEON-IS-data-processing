#!/usr/bin/env python3
import math
import pandas as pd

from flow_sae_trst_dp0p.pressuretransducer import PressureTransducer


class PressureTransducerCylinder(PressureTransducer):

    """
    specific class for ECSE presValiRegInStor (cylinder pressure 00111)
    which includes cylinder pressure flag
    output terms include presDiff and qfPresDiff
    """
    gas_threshold = 2757903

    def data_conversion(self, filename) -> pd.DataFrame:
        outputdf = super().data_conversion(filename)
        outputdf['qfPresDiff'] = outputdf['presDiff'].apply(lambda x: -1 if math.isnan(x) else self.get_qf_pressure(x))

        return outputdf

    def get_qf_pressure(self, pressure: float) -> int:
        if pressure < self.gas_threshold:
            return 1
        else:
            return 0


def main() -> None:

    """
    , applicable for data products
    of ECSE: presValiRegInStor
    """
    data_columns = {'pressure': 'presDiff'}
    presTrans = PressureTransducerCylinder(data_columns)
    presTrans.l0tol0p()


if __name__ == "__main__":
    main()
