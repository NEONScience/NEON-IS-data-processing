#!/usr/bin/env python3
import environs
import structlog
import cx_Oracle
from contextlib import closing
from pathlib import Path

import common.log_config as log_config
from data_access.asset_repository import AssetRepository
from data_access.named_location_repository import NamedLocationRepository

from location_asset_loader.location_asset_loader import LocationAssetLoader

log = structlog.get_logger()


def main():
    env = environs.Env()
    out_path: Path = env.path('OUT_PATH')
    db_url: str = env.str('DATABASE_URL')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    log_config.configure(log_level)
    log.info('Processing.')
    log.debug(f'out_path: {out_path}')

    with closing(cx_Oracle.connect(db_url)) as connection:
        named_location_repository = NamedLocationRepository(connection)
        asset_repository = AssetRepository(connection)
        location_asset_loader = LocationAssetLoader(named_location_repository=named_location_repository,
                                                    asset_repository=asset_repository,
                                                    out_path=out_path)
        location_asset_loader.process()


if __name__ == "__main__":
    main()
