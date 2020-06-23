#!/usr/bin/env python3
import os
from pathlib import Path
from typing import List

import unittest
import geojson
import json

from pyfakefs.fake_filesystem_unittest import TestCase

import common.date_formatter as date_formatter
import location_loader.location_loader as location_loader
import location_loader.location_loader_main as location_loader_main


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
        # expected data
        schema_name = 'prt'
        location = 'CFGLOC123'
        location_type = 'CONFIG'
        description = 'A test location.'
        site = 'CPER'

        # functions to pass to function under test
        def get_schema_name(named_location_name) -> str:
            """
            Return the schema name.

            :param named_location_name: A named location name.
            :return: The schema name
            """
            print(f'location name: {named_location_name}')
            return schema_name

        def get_locations(type_name: str, stop_date) -> List[geojson.FeatureCollection]:
            """
            Return a mock named location in GEOJson format.

            :param type_name: The named location type.
            :param stop_date: The cutoff date for the maximum active period.
            :return: The named location data.
            """
            end_date = date_formatter.convert(stop_date)

            active_periods = []
            active_period = {'start_date': '2020-01-01T00:00:00', 'end_date': end_date}
            active_periods.append(active_period)

            feature = geojson.Feature(properties={'name': location,
                                                  'type': type_name,
                                                  'description': description,
                                                  'site': site,
                                                  'context': schema_name,
                                                  'active_periods': active_periods})
            feature_collection = geojson.FeatureCollection([feature])
            return [feature_collection]

        # test the function
        cutoff_date = date_formatter.parse('2020-01-03T00:00:00Z')
        location_loader.write_files(location_type=location_type, cutoff_date=cutoff_date, out_path=self.out_path,
                                    get_locations=get_locations, get_schema_name=get_schema_name)

        # check output
        file_path = Path(self.out_path, schema_name, location, f'{location}.json')
        self.assertTrue(file_path.exists())
        with open(file_path) as file:
            file_data = geojson.load(file)
            geojson_data = geojson.dumps(file_data, indent=4, sort_keys=False, default=str)
            print(f'file geojson_data: {geojson_data}')
            json_data = json.loads(geojson_data)
            properties = json_data['features'][0]['properties']
            self.assertTrue(properties['name'] == location)
            self.assertTrue(properties['type'] == location_type)
            self.assertTrue(properties['description'] == description)
            self.assertTrue(properties['site'] == site)
            self.assertTrue(properties['context'] == schema_name)
