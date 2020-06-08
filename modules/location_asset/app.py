#!/usr/bin/env python3
from pathlib import Path

import geojson
import environs
import structlog
import cx_Oracle
from contextlib import closing

import common.log_config as log_config
from data_access.named_location_repository import NamedLocationRepository
from data_access.asset_repository import AssetRepository

log = structlog.get_logger()


def process(db_url: str, out_path):
    """
    Loop over assets and write the asset's location history to a file.

    :param out_path: The output path for writing files.
    :param db_url: The database connection URL.
    """
    with closing(cx_Oracle.connect(db_url)) as connection:
        named_location_repository = NamedLocationRepository(connection)
        asset_repository = AssetRepository(connection)
        assets = asset_repository.get_all()
        for asset in assets:
            log.debug(f'Processing asset: {asset}')
            if asset['asset_type'] is not None:
                asset_location_history = named_location_repository.get_asset_history(asset_id)
                write_file(asset, asset_location_history, out_path)
            else:
                asset_id = asset['asset_id']
                log.error(f'Asset {asset_id} has no type defined.')


def write_file(asset: dict, asset_location_history: geojson.FeatureCollection, out_path: Path):
    asset_id = asset['asset_id']
    asset_type = asset['asset_type']
    # add the asset to the location history
    asset_location_history.update({"source_type": asset_type})
    asset_location_history.update({"source_id": asset_id})
    # write geojson file
    geojson_data = geojson.dumps(asset_location_history, indent=4, sort_keys=False, default=str)
    file_name = f'{asset_type}_{str(asset_id)}_locations.json'
    file_path = Path(out_path, asset_type, str(asset_id), file_name)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, 'w') as location_file:
        location_file.write(geojson_data)


def main():
    env = environs.Env()
    out_path = env.path('OUT_PATH')
    db_url = env.str('DATABASE_URL')
    log_level = env.log_level('LOG_LEVEL', 'INFO')
    log_config.configure(log_level)
    log.info('Processing.')
    log.debug(f'out_path: {out_path}')
    process(db_url, out_path)


if __name__ == "__main__":
    main()
