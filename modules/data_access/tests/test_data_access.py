import os

import geojson
import cx_Oracle
import unittest
from pyfakefs.fake_filesystem_unittest import TestCase

import data_access.asset_finder as asset_finder
import data_access.named_location_finder as named_location_finder


class AppTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = os.path.join('/', 'output')
        #  Set output directory in mock filesystem.
        self.fs.create_dir(self.out_path)
        #  Database URL in the form: [user]/[pass]@[url]:[port]/[sid]
        database_url = os.getenv('DATABASE_URL')
        self.connection = cx_Oracle.connect(database_url)

    def test_asset_finder(self):
        result = asset_finder.find_all(self.connection)
        self.assertTrue((result is not None))

    @unittest.skip('Skip due to long process time.')
    def test_type_context(self):
        locations = named_location_finder.get_type_context(self.connection, 'CONFIG', 'soil')
        for location in locations:
            geojson_data = geojson.dumps(location, indent=4, sort_keys=False, default=str)
            print(f'geojson_data: {geojson_data}')
        self.assertTrue((locations is not None))

    def test_get_site(self):
        site_name = named_location_finder.get_site(self.connection, 31720)
        self.assertTrue(site_name == 'ORNL')

    def test_range(self):
        for i in range(1, 11):
            print(f'i: {i}')
        self.assertTrue(i == 10)

    def tearDown(self):
        self.connection.close()
