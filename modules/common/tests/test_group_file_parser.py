#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

import common.group_file_parser as group_file_parser


class GroupFileParserTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.group_file_path = Path('/group-member-group.json')
        actual_group_file_path = Path(os.path.dirname(__file__), 'test-group-member-group.json')
        self.fs.add_real_file(actual_group_file_path, target_path=self.group_file_path)

    def test_parse_group_file(self):
        (name, group, active_periods, hor, ver) = group_file_parser.parse_group_file(self.group_file_path)
        self.assertTrue(name[0] == 'rel-humidity_HARV003000')
        self.assertTrue(group[1] == 'pressure-air_HARV000060')
        active_period = active_periods[1][0]
        self.assertTrue(active_period['start_date'] == '2020-01-01T00:00:00Z')
        self.assertTrue(active_period['end_date'] == '2020-01-02T00:00:00Z')
        self.assertTrue(hor[1] == '000')
        self.assertTrue(ver[1] == '060')
        
    def test_get_group(self):
        group = group_file_parser.get_group(self.group_file_path)
        self.assertTrue(group[1] == 'pressure-air_HARV000060')

    def test_get_group_matches(self):
        group = group_file_parser.get_group(self.group_file_path)
        matches = group_file_parser.get_group_matches(group,"pressure-air_HARV0000")
        self.assertTrue(matches[0] == 'pressure-air_HARV000060')
