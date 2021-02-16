#!/usr/bin/env python3
import os
from typing import List, Set
import unittest
import json

import cx_Oracle
from geojson import FeatureCollection
from geojson import dumps as geojson_dumps

from data_access.get_asset_locations import get_asset_locations
from data_access.get_assets import get_assets
from data_access.get_named_location_active_periods import get_active_periods
from data_access.get_named_location_context import get_named_location_context
from data_access.get_named_location_locations import get_named_location_locations
from data_access.get_named_location_properties import get_named_location_properties
from data_access.get_named_location_schema_name import get_named_location_schema_name
from data_access.get_named_location_site import get_named_location_site
from data_access.get_named_locations import get_named_locations
from data_access.get_thresholds import get_thresholds
from data_access.types.active_period import ActivePeriod
from data_access.types.asset import Asset
from data_access.types.property import Property


class DataAccessTest(unittest.TestCase):

    def setUp(self):
        # database URL in the form: [user]/[pass]@[url]:[port]/[sid]
        db_url = os.getenv('DATABASE_URL')
        self.connection = cx_Oracle.connect(db_url)
        self.named_location_id = 31720

    def test_get_asset_locations(self):
        # asset = Asset(id=41283, type='windobserverii')
        asset = Asset(id=18521, type='prt')  # soil plot test
        feature_collection: FeatureCollection = get_asset_locations(self.connection, asset)
        geojson_data = geojson_dumps(feature_collection, indent=4, sort_keys=False, default=str)
        # print(f'asset_locations: \n{geojson_data}')
        self.assertTrue(feature_collection is not None)

    def test_get_assets(self):
        i = 0
        asset = None
        for asset in get_assets(self.connection):
            if i > 0:
                break
            # print(f'asset id: {asset.id} type: {asset.type}')
            i += 1
        self.assertTrue(asset is not None)

    def test_get_named_location_active_periods(self):
        active_periods: List[ActivePeriod] = get_active_periods(self.connection, self.named_location_id)
        self.assertTrue(active_periods is not None)

    def test_get_named_location_context(self):
        context: List[str] = get_named_location_context(self.connection, self.named_location_id)
        expected_context = ['par-met-343', 'par-met', 'upward-facing']
        print(f'context: {context}')
        self.assertTrue(context == expected_context)

    def test_get_named_location_locations(self):
        # Point geometry
        result = get_named_location_locations(self.connection, self.named_location_id)
        # print(f'result: {json.dumps(result, indent=2)}')
        # Polygon geometry
        result = get_named_location_locations(self.connection, 314)
        # print(f'result: {json.dumps(result, indent=2)}')
        self.assertTrue(result is not None)

    def test_get_named_location_properties(self):
        properties: List[Property] = get_named_location_properties(self.connection, self.named_location_id)
        prop = properties[0]
        self.assertTrue(prop.name == 'HOR')
        self.assertTrue(prop.value == '000')

    def test_get_named_location_schema_name(self):
        named_location_id = 156951
        schema_names: Set = get_named_location_schema_name(self.connection, named_location_id)
        # print(f'schema_names: {schema_names}')
        self.assertTrue(next(iter(schema_names)) == 'windobserverii')

    def test_get_named_location_site(self):
        site = get_named_location_site(self.connection, self.named_location_id)
        self.assertTrue(site == 'ORNL')

    def test_get_named_locations(self):
        location = None
        i = 0
        for location in get_named_locations(self.connection, 'CONFIG'):
            if i > 0:
                break
            i += 1
        # print(f'location: {location}')
        self.assertTrue(location is not None)

    def test_get_thresholds(self):
        i = 0
        for threshold in get_thresholds(self.connection):
            if i > 0:
                break
            # print(f'threshold: {threshold}')
            i += 1
            self.assertTrue(threshold is not None)

    def tearDown(self):
        self.connection.close()
