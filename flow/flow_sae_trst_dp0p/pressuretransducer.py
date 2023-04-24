#!/usr/bin/env python3
import math
import pandas as pd

from flow_sae_trst_dp0p.l0tol0p import L0toL0p
from flow_sae_trst_dp0p.shared_functions import get_pressure_pa
from structlog import get_logger

log = get_logger()


class PressureTransducer(L0toL0p):

    GAS_THRESHOLD = 2757903
    PRESS_LOW = 0.4
    PRESS_HIGH = 0.5
    ambient_press = 9999  # TODO

    def data_conversion(self, filename) -> pd.DataFrame:
        df = super(PressureTransducer, self).data_conversion(filename)
        df['pressure'] = df['pressure'].apply(lambda x: math.nan if math.isnan(x) else get_pressure_pa(x))

        # if self.new_source_type_name in ['presValiRegInTurb', 'presValiRegOutTurb', 'presValiLine']:
        #    # ECTE: presValiRegInTurb 00034, presValiRegOutTurb 00035, presValiLineTurb 00037
        #    # ECSE: presValiRegOutStor 00110 (delivery pressure, excludes pressure from zero to crd h2o: HOR-711)
        data_columns = {'pressure': 'presDiff'}
        if self.new_source_type_name == 'presTrap':
            # ECTE: presTrap 00036, output term is presAtm
            data_columns['pressure'] = 'presAtm'
        elif self.new_source_type_name == 'presValiRegInStor':
            # ECSE cylinder pressure 00111 with cylinder pressure flag qfPresDiff
            df['qfPresDiff'] = df['pressure'].apply(lambda x: -1 if math.isnan(x) else self.get_qf_pressure_reginstor(x))
        elif self.new_source_type_name == 'presValiRegOutStor_crdH2o':
            pass
            # TODO needs implementation for crdh2o, how to distinguish this from normal 00110 ?
        elif self.new_source_type_name == 'presInlt':
            # ECSE: presInlt 00109
            # TODO: get ambient pressure from location file?
            # not this: self.ambient_press = get_site_presAtm(df['site'][0])
            # df['qfPresDiff'] = df['presDiff'].apply(lambda x: -1 if math.isnan(x) else self.get_qf_pressure_inlt(x))
            pass

        df.rename(columns=data_columns, inplace=True)
        log.debug(f'{df.columns}')

        return df

    def get_qf_pressure_reginstor(self, pressure: float) -> int:
        if pressure < self.GAS_THRESHOLD:
            return 1
        else:
            return 0

    def get_qf_pressure_inlt(self, pressure: float) -> int:
        if pressure < self.PRESS_LOW * self.ambient_press or pressure > self.PRESS_HIGH * self.ambient_press:
            return 1
        else:
            return 0


def main() -> None:

    """
    output term is presDiff, applicable for data products
    of ECTE: presValiRegInTurb 00034, presValiRegOutTurb 00035, presValiLineTurb 00037
    and ECSE: presValiRegOutStor 00110 (delivery pressure, excludes pressure from zero to crd h2o: HOR-711)
    """
    context_dp_map = {'presTrap': 'presTrap', 'presValiLine': 'presValiLine',
                      'presValiRegIn,turbulent': 'presValiRegInTurb',
                      'presValiRegOut,turbulent': 'presValiRegOutTurb',
                      'presValiRegIn,storage': 'presValiRegInStor',
                      'presValiRegOut,storage': 'presValiRegOutStor',
                      'presInlt': 'presInlt'}

    # default output term is presDiff
    prestrans = PressureTransducer(context_dp_map=context_dp_map)
    prestrans.l0tol0p()


if __name__ == "__main__":
    main()
