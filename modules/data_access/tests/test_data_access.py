#!/usr/bin/env python3
import os
from pathlib import Path
from datetime import datetime

import geojson
import cx_Oracle
import unittest
from pyfakefs.fake_filesystem_unittest import TestCase

from data_access.active_period_repository import ActivePeriodRepository
from data_access.asset_repository import AssetRepository
from data_access.named_location_repository import NamedLocationRepository
from data_access.named_location_context_repository import NamedLocationContextRepository
from data_access.named_location_parent_repository import NamedLocationParentRepository
from data_access.property_repository import PropertyRepository


class DataAccessTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = Path('/output')
        self.fs.create_dir(self.out_path)
        #  Database URL in the form: [user]/[pass]@[url]:[port]/[sid]
        database_url = os.getenv('DATABASE_URL')
        self.connection = cx_Oracle.connect(database_url)
        self.active_period_repository = ActivePeriodRepository(self.connection)
        self.asset_repository = AssetRepository(self.connection)
        self.named_location_repository = NamedLocationRepository(self.connection)
        self.named_location_context_repository = NamedLocationContextRepository(self.connection)
        self.named_location_parent_repository = NamedLocationParentRepository(self.connection)
        self.property_repository = PropertyRepository(self.connection)
        self.named_location_id = 31720

    def test_asset_finder(self):
        result = self.asset_repository.get_all()
        self.assertTrue((result is not None))

    @unittest.skip('Skip due to long process time.')
    def test_type_context(self):
        locations = self.named_location_repository.get_by_type('CONFIG', datetime.now())
        for location in locations:
            geojson_data = geojson.dumps(location, indent=4, sort_keys=False, default=str)
            print(f'geojson_data: {geojson_data}')
        self.assertTrue((locations is not None))

    def test_get_site(self):
        site_name = self.named_location_repository.get_site(self.named_location_id)
        self.assertTrue(site_name == 'ORNL')

    def test_get_schema_name(self):
        named_location_name = 'SENSOR000000'
        schema_name = self.named_location_repository.get_schema_name(named_location_name)
        self.assertTrue(schema_name == 'exo2')

    def test_get_active_periods(self):
        active_periods = self.active_period_repository.get_active_periods(self.named_location_id)
        self.assertTrue(active_periods is not None)

    def test_context_repository(self):
        contexts = self.named_location_context_repository.get_context(self.named_location_id)
        self.assertTrue(len(contexts) == 0)

    def test_parent_repository(self):
        parents = self.named_location_parent_repository.get_parents(self.named_location_id)
        self.assertTrue(parents[0]['name'] == 'ORNL')

    def test_property_repository(self):
        properties = self.property_repository.get_named_location_properties(self.named_location_id)
        prop = properties[0]
        for key, value in prop.items():
            print(prop)
            self.assertTrue(key == 'Required Asset Management Location Code')
            self.assertTrue(value == 'CFGLOC100805')

    def tearDown(self):
        self.connection.close()
