#!/usr/bin/env python3
import os
from pathlib import Path

from geojson import Feature, FeatureCollection
import unittest

from pyfakefs.fake_filesystem_unittest import TestCase

import location_asset_loader.location_asset_loader_main as location_asset_loader_main
from location_asset_loader.location_asset_loader import LocationAssetLoader


class LocationAssetLoaderTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = Path('/output')
        self.fs.create_dir(self.out_path)
        #  Database URL in the form: [user]/[pass]@[url]:[port]/[sid]
        self.database_url = os.getenv('DATABASE_URL')

    @unittest.skip('Skip due to long process time.')
    def test_app(self):
        os.environ['DATABASE_URL'] = self.database_url
        os.environ['OUT_PATH'] = self.out_path
        os.environ['LOG_LEVEL'] = 'DEBUG'
        location_asset_loader_main.main()
        self.check_output()

    def test_write_file(self):
        asset = {'asset_id': 2201, 'asset_type': 'prt'}
        location = Feature(geometry=None,
                           properties={'start_date': '2020-01-01T00:00:00Z',
                                       'end_date': '2020-01-02T00:00:00Z',
                                       'transaction_date': '2020-01-03T00:00:00Z',
                                       'alpha': 'alpha',
                                       'beta': 'beta',
                                       'gamma': 'gamma',
                                       'x_offset': 'x_offset',
                                       'y_offset': 'y_offset',
                                       'z_offset': 'z_offset',
                                       'reference_location': 'reference_location'})
        feature = Feature(properties={'name': 'CFGLOC123',
                                      'site': 'CPER',
                                      'install_date': '2020-01-01T00:00:00Z',
                                      'remove_date': '2020-01-02T00:00:00Z',
                                      'transaction_date': '2020-01-03T00:00:00Z',
                                      'context': 'aspirated-triple',
                                      'locations': location})
        features = [feature]
        asset_location_history = FeatureCollection(features)
        LocationAssetLoader.write_file(asset, asset_location_history, self.out_path)
        self.check_output()

    def check_output(self):
        expected_path = self.out_path.joinpath('prt/2201/prt_2201_locations.json')
        self.assertTrue(expected_path.exists())
