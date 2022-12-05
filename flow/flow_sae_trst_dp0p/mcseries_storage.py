#!/usr/bin/env python3
from structlog import get_logger

import math
import pandas as pd

from flow_sae_trst_dp0p.mcseries_turb import McseriesTurb

log = get_logger()


class McseriesStorage(McseriesTurb):

    def data_conversion(self, filename) -> pd.DataFrame:
        df = super().data_conversion(filename)
        df['qfFrt00'] = df['frt00'].apply(lambda x: -1 if math.isnan(x) else self.get_qf_frt00(x)).astype('int8')

        return df

    def get_qf_frt00(self, frt00: float) -> int:
        if frt00 < 0.8 * self.CONV_CONST or frt00 > 1.2 * self.CONV_CONST:
            return 1
        else:
            return 0


def main() -> None:

    mcseries = McseriesStorage()
    mcseries.l0tol0p()


if __name__ == "__main__":
    main()
