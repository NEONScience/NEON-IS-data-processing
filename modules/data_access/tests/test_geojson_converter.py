#!/usr/bin/env python3
import json
from geojson import FeatureCollection, Feature, dumps
import unittest

import common.date_formatter as date_formatter
import data_access.types.geojson_converter as geojson_converter
from data_access.types.asset_location import AssetLocation
from data_access.types.active_period import ActivePeriod
from data_access.types.named_location import NamedLocation
from data_access.types.property import Property


class GeoJsonConverterTest(unittest.TestCase):

    def test_convert_asset_location(self):
        name = 'CFG123'
        site = 'CPER'
        install_date = '2020-01-01T00:00:00Z'
        remove_date = '2020-01-02T00:00:00Z'
        context = 'context'
        install_datetime = date_formatter.to_datetime(install_date)
        remove_datetime = date_formatter.to_datetime(remove_date)
        context_list = [context]
        properties = [Property(name='prop1', value='value1')]
        location_start_date = '2020-01-01T00:00:00Z'
        location_end_date = '2020-01-10T00:00:00Z'
        alpha = '1.0'
        beta = '2.0'
        gamma = '3.0'
        x_offset = '1.0'
        y_offset = '2.0'
        z_offset = '3.0'
        location_properties = {'start_date': location_start_date,
                               'end_date': location_end_date,
                               'alpha': alpha,
                               'beta': beta,
                               'gamma': gamma,
                               'x_offset': x_offset,
                               'y_offset': y_offset,
                               'z_offset': z_offset,
                               'reference_location': None}
        location = Feature(properties=location_properties)
        locations = FeatureCollection([location])
        asset_location = AssetLocation(name=name, site=site, install_date=install_datetime, remove_date=remove_datetime,
                                       context=context_list, properties=properties, locations=locations)
        feature = geojson_converter.convert_asset_location(asset_location)
        geojson_data = dumps(feature, indent=4, sort_keys=False, default=str)
        json_data = json.loads(geojson_data)
        print(f'{geojson_data}')
        properties = json_data['properties']
        self.assertTrue(properties['name'] == name)
        self.assertTrue(properties['site'] == site)
        self.assertTrue(properties['install_date'] == install_date)
        self.assertTrue(properties['remove_date'] == remove_date)
        self.assertTrue(properties['context'][0] == context)
        self.assertTrue(properties['prop1'] == 'value1')
        locations = properties['locations']
        location = locations['features'][0]
        geojson_location_properties = location['properties']
        self.assertTrue(geojson_location_properties['start_date'] == location_start_date)
        self.assertTrue(geojson_location_properties['end_date'] == location_end_date)
        self.assertTrue(geojson_location_properties['alpha'] == alpha)
        self.assertTrue(geojson_location_properties['beta'] == beta)
        self.assertTrue(geojson_location_properties['gamma'] == gamma)
        self.assertTrue(geojson_location_properties['x_offset'] == x_offset)
        self.assertTrue(geojson_location_properties['y_offset'] == y_offset)
        self.assertTrue(geojson_location_properties['z_offset'] == z_offset)

    def test_convert_named_location(self):
        name = 'CFG123'
        location_type = 'CONFIG'
        description = 'A named location.'
        site = 'CPER'
        start_date = '2020-01-01T00:00:00Z'
        end_date = '2020-01-02T00:00:00Z'
        context = 'context'
        active_period = ActivePeriod(start_date=date_formatter.to_datetime(start_date),
                                     end_date=date_formatter.to_datetime(end_date))
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
        self.assertTrue(feature['prop1'] == 'value1')
