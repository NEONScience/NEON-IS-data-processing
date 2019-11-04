import os
import pathlib
from datetime import datetime

import geojson
import environs
import structlog
import cx_Oracle
from contextlib import closing

import data_access.named_location_finder as named_location_finder
import data_access.asset_finder as asset_finder
import lib.date_formatter as date_formatter
import lib.log_config as log_config


log = structlog.get_logger()


def write_file(out_path, asset, location_history, date_generated):
    """
    Write the input asset and location history into a geojson file in the output directory.
    :param out_path: Directory to write the file.
    :param asset: Dictionary containing asset type and ID.
    :param location_history: Dictionary containing the asset location history.
    :param date_generated: The file generation date.
    """
    pathlib.Path(out_path).mkdir(parents=True, exist_ok=True)

    source_type = asset.get('asset_type')
    source_id = str(asset.get('asset_id'))

    location_history.update({"source_type": asset.get('asset_type')})
    location_history.update({"source_id": asset.get('asset_id')})
    location_history.update({"document_generated_date": date_generated})

    geojson_data = geojson.dumps(location_history, indent=4, sort_keys=False, default=str)
    file_name = source_type + '_' + source_id + '_' + 'locations.json'
    file_path = os.path.join(out_path, file_name)
    with open(file_path, 'w') as outfile:
        outfile.write(geojson_data)


def load(db_url, out_path):
    """Loop over assets and write the asset and location history to a file."""
    with closing(cx_Oracle.connect(db_url)) as connection:
        assets = asset_finder.find_all(connection)
        for asset in assets:
            log.debug(f'Processing asset {asset}')
            asset_type = asset['asset_type']
            asset_id = asset['asset_id']
            if asset_type is not None:
                asset_out_dir = os.path.join(out_path, asset_type, str(asset_id))
                location_history = named_location_finder.get_asset_history(connection, asset_id)
                date_generated = date_formatter.convert(datetime.utcnow())
                write_file(asset_out_dir, asset, location_history, date_generated)
            else:
                log.error(f'Type for asset {asset_id} is not defined.')


def main():
    env = environs.Env()
    out_path = env('OUT_PATH')
    db_url = env('DATABASE_URL')
    log_level = env('LOG_LEVEL')
    log_config.configure(log_level)
    log.debug(f'Out path: {out_path}')
    load(db_url, out_path)


if __name__ == "__main__":
    main()
