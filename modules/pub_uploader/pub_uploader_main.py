#!/usr/bin/env python3
from pathlib import Path

import environs
import structlog
from contextlib import closing
import os
import pandas as pd
import common.log_config as log_config
from data_access.db_config_reader import read_from_mount
from data_access.db_connector import DbConnector
from data_access.create_pub import create_pub

log = structlog.get_logger()


def main() -> None:
    env = environs.Env()
    version = os.environ['VERSION']
    change_by = env.str('CHANGE_BY')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    log_config.configure(log_level)
    data_path: Path = env.path('DATA_PATH')
    starting_path_index: int = env.int('STARTING_PATH_INDEX')
    db_config = read_from_mount(Path('/var/db_secret'))
    
    with closing(DbConnector(db_config)) as connector:
        # Find each manifest file
        data_path_start = Path(*data_path.parts[0:starting_path_index+1]) # starting index
        for path in data_path_start.rglob('*'):
            # When we reach a manifest file, process it
            if path.parts[-1] == 'manifest.csv':
                log.info(f'Processing manifest {path}') 
                manifest = pd.read_csv(path)
                create_pub(connector, manifest, version, change_by)


if __name__ == "__main__":
    main()
