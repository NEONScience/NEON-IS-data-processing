#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

from common.location_file_parser import get_active_periods


class LocationFileParserTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.location_path = Path('/test-location.json')
        actual_location_file_path = Path(os.path.dirname(__file__), 'test-location.json')
        self.fs.add_real_file(actual_location_file_path, target_path=self.location_path)

    def test_get_active_periods(self):
        name, active_periods = get_active_periods(self.location_path)
        active_period = active_periods[0]
        self.assertTrue(name == 'SENSOR000202')
        self.assertTrue(active_period['start_date'] == '2018-09-01T00:00:00Z')
        self.assertTrue(active_period['end_date'] == '2020-06-17T00:00:00Z')
