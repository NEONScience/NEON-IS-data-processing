#!/usr/bin/env python3
from pathlib import Path
from typing import Callable, List

from geojson import FeatureCollection, dumps
import structlog

from data_access.asset import Asset

log = structlog.get_logger()


def write_files(*, get_assets: Callable[[], List[Asset]],
                get_location_history: Callable[[int], FeatureCollection], out_path: Path):
    """
    Write assets and their location history to GEOJson files.

    :param get_assets: Function to return a list of assets.
    :param get_location_history: Function to return asset location history.
    :param out_path: Path for writing files.
    """
    for asset in get_assets():
        log.debug(f'Processing asset: {asset}')
        if asset.type is not None:
            history = get_location_history(asset.id)
            # add the asset to the location history as the "source"
            history.update({"source_id": asset.id})
            history.update({"source_type": asset.type})
            write_file(asset=asset, history=history, out_path=out_path)
        else:
            # some types may not be available
            log.error(f'Asset {asset.id} has no type defined.')


def write_file(*, asset: Asset, history: FeatureCollection, out_path: Path):
    """
    Write a GEOJson file of the asset.

    :param asset: The asset.
    :param history: The asset's location history.
    :param out_path: The path for writing files.
    """
    geojson_data = dumps(history, indent=4, sort_keys=False, default=str)
    file_name = f'{asset.type}_{str(asset.id)}_locations.json'
    file_path = Path(out_path, asset.type, str(asset.id), file_name)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, 'w') as location_file:
        location_file.write(geojson_data)
