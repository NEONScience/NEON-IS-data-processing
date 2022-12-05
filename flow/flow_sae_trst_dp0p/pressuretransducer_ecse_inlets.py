#!/usr/bin/env python3
import math
import pandas as pd
from flow_sae_trst_dp0p.shared_functions import get_site_presAtm
from flow_sae_trst_dp0p.pressuretransducer import PressureTransducer


class PressureTransducerInlet(PressureTransducer):

    """
    specific class for ECSE pressure inlets includes pressure flag
    """
    press_low = 0.4
    press_high = 0.5

    def data_conversion(self, filename) -> pd.DataFrame:
        outputdf = super().data_conversion(filename)
        self.ambient_press = get_site_presAtm(outputdf['site'][0])
        outputdf['qfPresDiff'] = outputdf['presDiff'].apply(lambda x: -1 if math.isnan(x) else self.get_qf_pressure(x))

        return outputdf

    def get_qf_pressure(self, pressure: float) -> int:
        if pressure < self.press_low * self.ambient_press or pressure > self.press_high * self.ambient_press:
            return 1
        else:
            return 0


def main() -> None:

    """
    output terms include presDiff and qfPresDiff, applicable for data products
    of ECSE: presInlt 00109
    """
    data_columns = {'pressure': 'presDiff'}
    presTrans = PressureTransducerInlet(data_columns)
    presTrans.l0tol0p()


if __name__ == "__main__":
    main()
