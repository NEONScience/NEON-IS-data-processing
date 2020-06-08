#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

from common.location_file_parser import LocationFileParser


class LocationFileParserTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.location_path = Path('/test-location.json')
        actual_location_file_path = Path(os.path.dirname(__file__), 'test-location.json')
        self.fs.add_real_file(actual_location_file_path, target_path=self.location_path)
        self.location_file_parser = LocationFileParser(self.location_path)

    def test_contains_context(self):
        has_match = self.location_file_parser.contains_context('aspirated-triple')
        self.assertTrue(has_match)
        no_match = self.location_file_parser.contains_context('aspirated-single')
        self.assertFalse(no_match)

    def test_matching_context_items(self):
        matching_items = self.location_file_parser.matching_context_items('aspirated-triple')
        self.assertTrue(len(matching_items) == 1)
        no_items = self.location_file_parser.matching_context_items('aspirated-single')
        self.assertTrue(len(no_items) == 0)
