#!/usr/bin/env python3
import os
from pathlib import Path

from geojson import Feature, FeatureCollection, load
import unittest

from pyfakefs.fake_filesystem_unittest import TestCase

from data_access.types.asset import Asset

import location_asset_loader.location_asset_loader_main as location_asset_loader_main
import location_asset_loader.location_asset_loader as location_asset_loader


class LocationAssetLoaderTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = Path('/out')
        self.fs.create_dir(self.out_path)

        self.source_type = 'prt'
        self.source_id = 2201
        self.filename = f'{self.source_type}_{self.source_id}_locations.json'
        self.expected_path = Path(self.out_path, self.source_type, str(self.source_id), self.filename)

        self.asset = Asset(self.source_id, self.source_type)
        self.context = 'aspirated-triple'
        self.start_date = '2020-01-01T00:00:00Z'
        self.end_date = '2020-01-02T00:00:00Z'
        self.transaction_date = '2020-01-03T00:00:00Z'
        self.location_name = 'CFGLOC123'
        self.site = 'CPER'
        self.alpha = '0.1'
        self.beta = '0.2'
        self.gamma = '0.3'
        self.x_offset = '0.4'
        self.y_offset = '0.5'
        self.z_offset = '0.6'
        self.reference_location = 'reference_location_value'
        self.location = Feature(geometry=None,
                                properties={'start_date': self.start_date,
                                            'end_date': self.end_date,
                                            'transaction_date': self.transaction_date,
                                            'alpha': self.alpha,
                                            'beta': self.beta,
                                            'gamma': self.gamma,
                                            'x_offset': self.x_offset,
                                            'y_offset': self.y_offset,
                                            'z_offset': self.z_offset,
                                            'reference_location': self.reference_location})
        self.feature = Feature(properties={'name': self.location_name,
                                           'site': self.site,
                                           'install_date': self.start_date,
                                           'remove_date': self.end_date,
                                           'transaction_date': self.transaction_date,
                                           'context': self.context,
                                           'locations': self.location})

    def get_assets(self):
        """Mock function."""
        return [self.asset]

    def get_asset_locations(self, asset):
        """Mock function."""
        print(f'id: {asset.id}')
        feature_collection = FeatureCollection([self.feature])
        # add the asset to the location history as the "source"
        feature_collection.update({"source_id": asset.id})
        feature_collection.update({"source_type": asset.type})
        return feature_collection

    @unittest.skip('Integration test skipped due to long process time.')
    def test_main(self):
        # database URL in the form: postgresql://[user]@[url]:[port]/[database_name]?password=[pass]
        database_url = os.getenv('PG_DATABASE_URL')
        os.environ['DATABASE_URL'] = database_url
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        location_asset_loader_main.main()
        self.assertTrue(self.expected_path.exists())

    def test_write_file(self):
        locations = FeatureCollection([self.feature])
        location_asset_loader.write_file(asset=self.asset, locations=locations, out_path=self.out_path)
        self.check_output()

    def test_write_files(self):
        location_asset_loader.write_files(get_assets=self.get_assets, get_asset_locations=self.get_asset_locations,
                                          out_path=self.out_path)
        self.check_output()

    def check_output(self):
        """Examine output file contents."""
        with open(self.expected_path, 'r') as file:
            geojson_data = load(file)
            print(f'geojson: {geojson_data}')
            feature = geojson_data['features'][0]
            properties = feature['properties']
            file_location_name = properties['name']
            file_site = properties['site']
            file_install_date = properties['install_date']
            file_remove_date = properties['remove_date']
            file_transaction_date = properties['transaction_date']
            file_context = properties['context']
            file_location = properties['locations']
            location_properties = file_location['properties']
            location_start_date = location_properties['start_date']
            location_end_date = location_properties['end_date']
            location_transaction_date = location_properties['transaction_date']
            location_alpha = location_properties['alpha']
            location_beta = location_properties['beta']
            location_gamma = location_properties['gamma']
            location_x_offset = location_properties['x_offset']
            location_y_offset = location_properties['y_offset']
            location_z_offset = location_properties['z_offset']
            location_reference_location = location_properties['reference_location']
            self.assertTrue(file_location_name == self.location_name)
            self.assertTrue(file_site == self.site)
            self.assertTrue(file_install_date == self.start_date)
            self.assertTrue(file_remove_date == self.end_date)
            self.assertTrue(file_transaction_date == self.transaction_date)
            self.assertTrue(file_context == self.context)
            self.assertTrue(location_start_date == self.start_date)
            self.assertTrue(location_end_date == self.end_date)
            self.assertTrue(location_transaction_date == self.transaction_date)
            self.assertTrue(location_alpha == self.alpha)
            self.assertTrue(location_beta == self.beta)
            self.assertTrue(location_gamma == self.gamma)
            self.assertTrue(location_x_offset == self.x_offset)
            self.assertTrue(location_y_offset == self.y_offset)
            self.assertTrue(location_z_offset == self.z_offset)
            self.assertTrue(location_reference_location == self.reference_location)
