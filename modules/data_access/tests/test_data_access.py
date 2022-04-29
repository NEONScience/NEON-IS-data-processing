#!/usr/bin/env python3
import json
import os
import unittest
from typing import List, Set

from geojson import FeatureCollection, dumps as geojson_dumps

from data_access.db_connector import connect
from data_access.get_asset_locations import get_asset_locations
from data_access.get_assets import get_assets
from data_access.get_named_location_active_periods import get_active_periods
from data_access.get_named_location_context import get_named_location_context
from data_access.get_named_location_group import get_named_location_group
from data_access.get_named_location_locations import get_named_location_locations
from data_access.get_named_location_properties import get_named_location_properties
from data_access.get_named_location_schema_name import get_named_location_schema_name
from data_access.get_named_location_parents import get_named_location_parents
from data_access.get_named_locations import get_named_locations
from data_access.get_thresholds import get_thresholds
from data_access.types.active_period import ActivePeriod
from data_access.types.asset import Asset
from data_access.types.property import Property


class DataAccessTest(unittest.TestCase):

    @staticmethod
    def print_geojson(feature_collection: FeatureCollection) -> None:
        geojson_data = geojson_dumps(feature_collection, indent=4, sort_keys=False, default=str)
        print(f'Geojson data: \n{geojson_data}')

    def setUp(self):
        # database URL in the form: postgresql://[user]@[url]:[port]/[database_name]?password=[pass]
        self.db_url = os.getenv('DATABASE_URL')
        if self.db_url is None:
            raise unittest.SkipTest('DATABASE_URL environment variable not set, skipping tests.')
        self.connection = connect(self.db_url)
        self.named_location_id = 31720

    def test_get_asset_locations(self):
        asset = Asset(id=18521, type='prt')  # soil plot test
        feature_collection: FeatureCollection = get_asset_locations(self.connection, asset)
        self.print_geojson(feature_collection)
        self.assertTrue(feature_collection is not None)

    def test_get_assets(self):
        i = 0
        asset = None
        for asset in get_assets(self.connection, source_type='prt'):
            if i > 0:
                break
            print(f'asset id: {asset.id} type: {asset.type}')
            i += 1
        self.assertTrue(asset is not None)

    def test_get_named_location_active_periods(self):
        active_periods: List[ActivePeriod] = get_active_periods(self.connection, self.named_location_id)
        self.assertTrue(active_periods is not None)

    @unittest.skip('These data are frequently updated, only use this test for specific cases.')
    def test_get_named_location_context(self):
        context: List[str] = get_named_location_context(self.connection, self.named_location_id)
        expected_context = ['par-met', 'upward-facing', 'par-met-351', 'ir-biological-temperature']
        self.assertTrue(context == expected_context)

    @unittest.skip('These data are frequently updated, only use this test for specific cases.')
    def test_get_named_location_group(self):
        group: List[str] = get_named_location_group(self.connection, self.named_location_id)
        expected_group = [376]
        self.assertTrue(group == expected_group)

    def test_get_named_location_locations(self):
        # Point geometry
        result = get_named_location_locations(self.connection, self.named_location_id)
        print(f'result: {json.dumps(result, indent=2)}')
        # Polygon geometry
        result = get_named_location_locations(self.connection, 314)
        print(f'result: {json.dumps(result, indent=2)}')
        self.assertTrue(result is not None)

    @unittest.skip('These data are frequently updated, only use this test for specific cases.')
    def test_get_named_location_properties(self):
        properties: List[Property] = get_named_location_properties(self.connection, self.named_location_id)
        prop = properties[4]
        print(f'prop: {prop}')
        self.assertTrue(prop.name == 'Required Asset Management Location ID')
        self.assertTrue(prop.value == 1834)
        self.assertTrue(type(prop.value) == int)

    def test_get_named_location_schema_name(self):
        named_location_id = 158818
        schema_names: Set = get_named_location_schema_name(self.connection, named_location_id)
        print(f'schema_names: {schema_names}')
        self.assertTrue(next(iter(schema_names)) == 'prt')

    def test_get_named_location_site(self):
        parents = get_named_location_parents(self.connection, self.named_location_id)
        site = parents['site']
        self.assertTrue(site == 'ORNL')

    def test_get_named_location_domain(self):
        parents = get_named_location_parents(self.connection, self.named_location_id)
        site = parents['domain']
        self.assertTrue(site == 'D07')

    def test_get_named_locations(self):
        location = None
        i = 0
        for location in get_named_locations(self.connection, 'CONFIG'):
            if i > 0:
                break
            i += 1
        print(f'location: {location}')
        self.assertTrue(location is not None)

    def test_get_thresholds(self):
        i = 0
        for threshold in get_thresholds(self.connection):
            if i > 0:
                break
            print(f'threshold: {threshold}')
            i += 1
            self.assertTrue(threshold is not None)

    def tearDown(self):
        self.connection.close()
