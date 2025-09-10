#!/usr/bin/env python3
from flow_sae_trst_dp0p.l0tol0p import L0toL0p

log = get_logger()

class Pump(L0toL0p):
    def data_conversion(self, filename) -> pd.DataFrame:
        df = super().data_conversion(filename)
        df['pumpVolt'] = df['dac_output'].apply(lambda x: math.nan if math.isnan(x) else x.astype('float32'))
        log.debug(f'{df.columns}')
        return df


def main() -> None:

    pump = Pump()
    pump.l0tol0p()
    log.debug("done.")


if __name__ == "__main__":
    main()
