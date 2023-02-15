#!/usr/bin/env python3
from pathlib import Path
from functools import partial
from contextlib import closing

from structlog import get_logger
import environs

import common.log_config as log_config
from data_access.get_readme import get_readme
from data_access.db_connector import DbConnector
from data_access.db_config_reader import read_from_mount

from readme_loader.readme_loader import load_readme


def main() -> None:
    env = environs.Env()
    out_path: Path = env.path('OUT_PATH')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    log_config.configure(log_level)
    log = get_logger()
    log.debug(f'out_path: {out_path}')
    db_config = read_from_mount(Path('/var/db_secret'))
    with closing(DbConnector(db_config)) as connector:
        get_readme_partial = partial(get_readme, connector=connector)
        load_readme(get_readme_partial, out_path)


if __name__ == "__main__":
    main()
