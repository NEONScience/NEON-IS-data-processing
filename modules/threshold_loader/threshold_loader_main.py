#!/usr/bin/env python3
from pathlib import Path
from functools import partial
from contextlib import closing

from structlog import get_logger
import environs

import common.log_config as log_config
from data_access.get_thresholds import get_thresholds
from data_access.db_connector import DbConnector

from threshold_loader.threshold_loader import load_thresholds


def main() -> None:
    env = environs.Env()
    out_path: Path = env.path('OUT_PATH')
    term: str = env.str('TERM')
    context: str = env.str('CTXT')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    log_config.configure(log_level)
    log = get_logger()
    log.debug(f'out_path: {out_path}')

    with closing(DbConnector()) as connector:
        get_thresholds_partial = partial(get_thresholds, connector=connector)
        load_thresholds(get_thresholds_partial, out_path, term=term, context=context)


if __name__ == "__main__":
    main()
