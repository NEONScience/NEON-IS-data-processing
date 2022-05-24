#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

import common.location_file_parser as location_file_parser


class LocationFileParserTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.location_file_path = Path('/test-location.json')
        self.location_group_file_path = Path('/test-location-group.json')
        actual_location_file_path = Path(os.path.dirname(__file__), 'test-location.json')
        self.fs.add_real_file(actual_location_file_path, target_path=self.location_file_path)
        actual_location_group_file_path = Path(os.path.dirname(__file__), 'test-location-group.json')
        self.fs.add_real_file(actual_location_group_file_path, target_path=self.location_group_file_path)

    def test_parse_location_file(self):
        (name, active_periods, context) = location_file_parser.parse_location_file(self.location_file_path)
        self.assertTrue(name == 'SENSOR000202')
        active_period = active_periods[0]
        self.assertTrue(active_period['start_date'] == '2018-09-01T00:00:00Z')
        self.assertTrue(active_period['end_date'] == '2020-06-17T00:00:00Z')
        self.assertTrue(context[0] == 'water-quality-296')
        
    def test_parse_location_group(self):
        (name, active_periods, context) = location_file_parser.parse_location_file(self.location_group_file_path)
        self.assertTrue(name == 'CFGLOC113711')
        self.assertTrue(context[0] == 'aspirated-single')
        group = location_file_parser.get_group(self.location_group_file_path)
        self.assertTrue(group[0] == 'aspirated-single_PUUM000010')
