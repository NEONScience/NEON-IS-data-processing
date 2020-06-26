#!/usr/bin/env python3
import json
from geojson import FeatureCollection, dumps
import unittest

import common.date_formatter as date_formatter
import data_access.types.geojson_converter as geojson_converter
from data_access.types.asset_location import AssetLocation
from data_access.types.active_period import ActivePeriod
from data_access.types.named_location import NamedLocation
from data_access.types.property import Property


class GeoJsonConverterTest(unittest.TestCase):

    def test_convert_location(self):
        self.assertTrue(True)

    @unittest.skip('Not yet complete.')
    def test_convert_asset_location(self):
        # TODO: Complete by adding location.
        name = 'CFG123'
        site = 'CPER'
        install_date = '2020-01-01T00:00:00Z'
        remove_date = '2020-01-02T00:00:00Z'
        transaction_date = '2020-01-03T00:00:00Z'
        context = 'context'
        install_date = date_formatter.parse(install_date)
        remove_date = date_formatter.parse(remove_date)
        transaction_date = date_formatter.parse(transaction_date)
        context = [context]
        properties = [Property(name=name, value='value')]
        locations = FeatureCollection()
        asset_location = AssetLocation(name=name, site=site, install_date=install_date, remove_date=remove_date,
                                       transaction_date=transaction_date, context=context, properties=properties,
                                       locations=locations)
        feature = geojson_converter.convert_asset_location(asset_location)
        print(f'feature: {feature}')
        self.assertTrue(True)

    def test_convert_named_location(self):
        name = 'CFG123'
        location_type = 'CONFIG'
        description = 'A named location.'
        site = 'CPER'
        start_date = '2020-01-01T00:00:00Z'
        end_date = '2020-01-02T00:00:00Z'
        context = 'context'
        active_period = ActivePeriod(start_date=date_formatter.parse(start_date),
                                     end_date=date_formatter.parse(end_date))
        prop = Property(name='prop1', value='value1')
        named_location = NamedLocation(name=name, type=location_type, description=description, site=site,
                                       schema_name='prt', context=[context], active_periods=[active_period],
                                       properties=[prop])
        feature = geojson_converter.convert_named_location(named_location)
        geojson_data = dumps(feature, indent=4, sort_keys=False, default=str)
        json_data = json.loads(geojson_data)
        print(f'location: {geojson_data}')
        feature = json_data['features'][0]
        properties = feature['properties']
        self.assertTrue(properties['name'] == name)
        self.assertTrue(properties['type'] == location_type)
        self.assertTrue(properties['description'] == description)
        self.assertTrue(properties['site'] == site)
        self.assertTrue(properties['description'] == description)
        self.assertTrue(properties['context'][0] == context)
        self.assertTrue(properties['active_periods'][0]['start_date'] == start_date)
        self.assertTrue(properties['active_periods'][0]['end_date'] == end_date)
        self.assertTrue(properties['prop1'] == 'value1')
