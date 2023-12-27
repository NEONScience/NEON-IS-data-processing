#!/usr/bin/env python3
import environs
import structlog
from pathlib import Path
from contextlib import closing
from functools import partial

import common.log_config as log_config
from data_access.db_config_reader import read_from_mount
from data_access.db_connector import DbConnector
from data_access.get_srf_loaders import get_srf_loaders
from srf_loader.srf_loader import load_srfs

log = structlog.get_logger()


def main() -> None:
    """Load the science review flag entries from the database and save to files."""
    env = environs.Env()
    group_prefix: str = env.str('GROUP_PREFIX')
    out_path: Path = env.path('OUT_PATH')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    log_config.configure(log_level)
    db_config = read_from_mount(Path('/var/db_secret'))
    with closing(DbConnector(db_config)) as connector:
        get_srfs_partial = partial(get_srf_loaders, connector=connector)
        load_srfs(out_path=out_path, get_srfs=get_srfs_partial, group_prefix=group_prefix)


if __name__ == "__main__":
    main()
