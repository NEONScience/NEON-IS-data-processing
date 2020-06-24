#!/usr/bin/env python3
import environs
import structlog
import cx_Oracle
from contextlib import closing
from pathlib import Path

import common.log_config as log_config
from data_access.asset_repository import AssetRepository
from data_access.named_location_repository import NamedLocationRepository

import location_asset_loader.location_asset_loader as location_asset_loader

log = structlog.get_logger()


def main():
    env = environs.Env()
    out_path: Path = env.path('OUT_PATH')
    db_url: str = env.str('DATABASE_URL')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    log_config.configure(log_level)
    log.debug(f'out_path: {out_path}')

    with closing(cx_Oracle.connect(db_url)) as connection:
        named_location_repository = NamedLocationRepository(connection)
        asset_repository = AssetRepository(connection)
        location_asset_loader.write_files(get_assets=asset_repository.get_assets,
                                          get_location_history=named_location_repository.get_asset_location_history,
                                          out_path=out_path)


if __name__ == "__main__":
    main()
