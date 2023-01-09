#!/usr/bin/env python3
from flow_sae_trst_dp0p.pressuretransducer import PressureTransducer


def main() -> None:

    """
    output term is presAtm, applicable for data products of ECTE: presTrap 00036
    """
    data_columns = {'pressure': 'presAtm'}
    presTrans = PressureTransducer(data_columns)
    presTrans.l0tol0p()


if __name__ == "__main__":
    main()
