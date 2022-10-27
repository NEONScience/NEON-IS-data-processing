#!/usr/bin/env python3
import environs
import structlog
from contextlib import closing
import os
import pandas as pd
import common.log_config as log_config
from data_access.db_connector import DbConnector
from data_access.create_pub import create_pub

log = structlog.get_logger()


def main() -> None:
    env = environs.Env()
    version = os.environ['VERSION']
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    log_config.configure(log_level)
    manifest_path = os.path.join(os.environ['DATA_PATH'], 'manifest.csv')
    manifest = pd.read_csv(manifest_path)
    with closing(DbConnector()) as connector:
        create_pub(connector, manifest, version)


if __name__ == "__main__":
    main()
