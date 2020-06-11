#!/usr/bin/env python3
from pathlib import Path

import geojson
import structlog

from data_access.named_location_repository import NamedLocationRepository
from data_access.asset_repository import AssetRepository

log = structlog.get_logger()


class LocationAssetLoader(object):

    def __init__(self, *,
                 named_location_repository: NamedLocationRepository,
                 asset_repository: AssetRepository,
                 out_path: Path):
        """
        Constructor.

        :param named_location_repository: Source for named location data.
        :param asset_repository: Source for asset data.
        :param out_path: Path for writing output files.
        """
        self.named_location_repository = named_location_repository
        self.asset_repository = asset_repository
        self.out_path = out_path

    def process(self):
        """Loop over assets and write the asset location history to a file. """
        assets = self.asset_repository.get_all()
        for asset in assets:
            log.debug(f'Processing asset: {asset}')
            asset_id = asset['asset_id']
            asset_type = asset['asset_type']
            if asset_type is not None:
                asset_location_history = self.named_location_repository.get_asset_history(asset_id)
                # add the asset to the location history as the "source"
                asset_location_history.update({"source_type": asset_type})
                asset_location_history.update({"source_id": asset_id})
                self.write_file(asset, asset_location_history, self.out_path)
            else:
                # some types have not yet been loaded in the database
                log.error(f'Asset {asset_id} has no type defined.')

    @staticmethod
    def write_file(asset: dict, asset_location_history: geojson.FeatureCollection, out_path: Path):
        asset_id = asset['asset_id']
        asset_type = asset['asset_type']
        geojson_data = geojson.dumps(asset_location_history, indent=4, sort_keys=False, default=str)
        file_name = f'{asset_type}_{str(asset_id)}_locations.json'
        file_path = Path(out_path, asset_type, str(asset_id), file_name)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, 'w') as location_file:
            location_file.write(geojson_data)
