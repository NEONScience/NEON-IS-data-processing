#!/usr/bin/env python3
import os
from pathlib import Path
from typing import List

import unittest
import geojson
import json

from pyfakefs.fake_filesystem_unittest import TestCase

from common.date_formatter import to_datetime
from data_access.types.named_location import NamedLocation
from data_access.types.property import Property
from data_access.types.active_period import ActivePeriod
import location_loader.location_loader as location_loader
import location_loader.location_loader_main as location_loader_main


class LocationLoaderTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = Path('/out')
        self.fs.create_dir(self.out_path)

    @unittest.skip('Integration test skipped due to long process time.')
    def test_main(self):
        # database URL in the form: postgresql://[user]@[url]:[port]/[database_name]?password=[pass]
        database_url = os.getenv('PG_DATABASE_URL')
        os.environ['LOCATION_TYPE'] = 'CONFIG'
        os.environ['DATABASE_URL'] = database_url
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        location_loader_main.main()
        file_path = Path(self.out_path, 'pqs1/CFGLOC100243/CFGLOC100243.json')
        self.assertTrue(file_path.exists())

    @unittest.skip('Test skipped.')
    def test_location_loader(self):
        site = 'CPER'
        location = 'CFGLOC123'
        schema_names = set('prt')
        description = 'A test location.'
        expected_type = 'CONFIG'

        def get_locations() -> List[NamedLocation]:
            """
            Return a mock named location.

            :return: The named location data.
            """
            prop = Property(name='property1', value='value1')
            active_period = ActivePeriod(start_date=to_datetime('2020-01-01T00:00:00Z'),
                                         end_date=to_datetime('2020-03-01T00:00:00Z'))
            named_location = NamedLocation(name=location,
                                           type=expected_type,
                                           description=description,
                                           site=site,
                                           schema_names=schema_names,
                                           context=list(schema_names),
                                           active_periods=[active_period],
                                           properties=[prop])
            return [named_location]

        # test
        location_loader.load_locations(out_path=self.out_path, get_locations=get_locations)
        # check output
        file_path = Path(self.out_path, next(iter(schema_names)), location, f'{location}.json')
        self.assertTrue(file_path.exists())
        with open(file_path) as file:
            file_data = geojson.load(file)
            geojson_data = geojson.dumps(file_data, indent=4, sort_keys=False, default=str)
            print(f'file: {geojson_data}')
            json_data = json.loads(geojson_data)
            properties = json_data['features'][0]['properties']
            self.assertTrue(properties['name'] == location)
            self.assertTrue(properties['type'] == expected_type)
            self.assertTrue(properties['description'] == description)
            self.assertTrue(properties['site'] == site)
            self.assertTrue(properties['context'][0] == next(iter(schema_names)))
