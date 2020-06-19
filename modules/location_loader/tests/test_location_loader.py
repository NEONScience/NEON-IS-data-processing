#!/usr/bin/env python3
import os
from pathlib import Path

import unittest
import geojson
import json

from pyfakefs.fake_filesystem_unittest import TestCase

from common.date_formatter import parse, convert

from location_loader.location_loader import LocationLoader
import location_loader.location_loader_main as location_loader_main


class NamedLocationRepositoryMock(object):

    def __init__(self):
        self.location = 'CFGLOC123'
        self.description = 'A test location.'
        self.schema_name = 'prt'
        self.site = 'CPER'

    def get_schema_name(self, name):
        return self.schema_name

    def get_by_type(self, location_type, cutoff_date):
        named_locations = []
        end_date = convert(cutoff_date)
        active_periods = []
        active_period = {'start_date': '2020-01-01T00:00:00', 'end_date': end_date}
        active_periods.append(active_period)
        features = []
        feature = geojson.Feature(properties={'name': self.location,
                                              'type': location_type,
                                              'description': self.description,
                                              'site': self.site,
                                              'context': self.schema_name,
                                              'active_periods': active_periods})
        features.append(feature)
        feature_collection = geojson.FeatureCollection(features)
        named_locations.append(feature_collection)
        return named_locations


class LocationLoaderTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = Path('/output')
        self.fs.create_dir(self.out_path)
        #  Database URL in the form: [user]/[pass]@[url]:[port]/[sid]
        self.database_url = os.getenv('DATABASE_URL')

    @unittest.skip('Skip due to long process time.')
    def test_main(self):
        os.environ['LOCATION_TYPE'] = 'CONFIG'
        os.environ['DATABASE_URL'] = self.database_url
        os.environ['tick'] = '/pfs/tick/2015-11-01T00:00:00Z'
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        location_loader_main.main()
        file_path = Path(self.out_path, 'prt/CFGLOC101740/CFGLOC101740.json')
        self.assertTrue(file_path.exists())

    def test_location_loader(self):
        repository = NamedLocationRepositoryMock()
        location_loader = LocationLoader(repository)
        cutoff_date = '2020-01-03T00:00:00Z'
        location_loader.load_files(location_type='CONFIG', cutoff_date=parse(cutoff_date), out_path=self.out_path)
        file_path = Path(self.out_path, repository.schema_name, repository.location, f'{repository.location}.json')
        self.assertTrue(file_path.exists())
        with open(file_path) as file:
            file_data = geojson.load(file)
            geojson_data = geojson.dumps(file_data, indent=4, sort_keys=False, default=str)
            # print(f'file geojson_data: {geojson_data}')
            json_data = json.loads(geojson_data)
            properties = json_data['features'][0]['properties']
            self.assertTrue(properties['name'] == repository.location)
            self.assertTrue(properties['description'] == repository.description)
            self.assertTrue(properties['site'] == repository.site)
