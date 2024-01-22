#!/usr/bin/env python3
from pathlib import Path
from typing import Callable, Iterator

from geojson import FeatureCollection, dumps
import structlog
import sys

from data_access.types.asset import Asset
from common.err_datum import err_datum_path

log = structlog.get_logger()


def write_files(*,
                get_assets: Callable[[str], Iterator[Asset]],
                get_asset_locations: Callable[[Asset], FeatureCollection],
                out_path: Path, err_path: Path,
                source_type: str) -> None:
    """
    Write GEOJson files of assets and their locations.

    :param get_assets: Function returning a list of assets.
    :param get_asset_locations: Function taking an Asset and returning a FeatureCollection of asset locations.
    :param out_path: Path for writing files.
    :param err_path: The error directory, i.e., /pfs/out/errored.
    :param source_type: The data source type.
    """
    for asset in get_assets(source_type=source_type):
        log.debug(f'Processing asset: {asset.id}')
        locations: FeatureCollection = get_asset_locations(asset=asset)
        write_file(asset=asset, locations=locations, out_path=out_path, err_path=err_path)


def write_file(*, asset: Asset, locations: FeatureCollection, out_path: Path, err_path: Path) -> None:
    """
    Write a GEOJson file of the asset and its locations.

    :param asset: The asset.
    :param locations: The asset's location history.
    :param out_path: The path for writing files.
    :param err_path: The error directory, i.e., /pfs/out/errored.
    """
    # DirErrBase: the user specified error directory, err_path
    DirErrBase = Path(err_path)
    dataDir_routed = Path("")
    geojson_data = dumps(locations, indent=4, sort_keys=True, default=str)
    file_name = f'{asset.type}_{str(asset.id)}_locations.json'
    file_path = Path(out_path, asset.type, str(asset.id), file_name)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    dataDir_routed = file_path.parent
    try:
        with open(file_path, 'w') as location_file:
            location_file.write(geojson_data)
    except:
        err_msg = sys.exc_info()
        err_datum_path(err=err_msg,DirDatm=str(dataDir_routed),DirErrBase=DirErrBase,
                       RmvDatmOut=True,DirOutBase=out_path)
                       
