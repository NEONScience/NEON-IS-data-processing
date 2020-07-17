#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

import common.location_file_parser as location_file_parser


class AssetLocationFileParserTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.asset_location_path = Path('/test-asset-location.json')
        actual_asset_location_file_path = Path(os.path.dirname(__file__), 'test-asset-location.json')
        self.fs.add_real_file(actual_asset_location_file_path, target_path=self.asset_location_path)
        self.context = location_file_parser.get_context(self.asset_location_path)

    def test_contains_context(self):
        has_match = 'aspirated-triple' in self.context
        self.assertTrue(has_match)
        no_match = 'aspirated-single' in self.context
        self.assertFalse(no_match)

    def test_matching_context_items(self):
        matching_items = location_file_parser.get_context_matches(self.context, 'aspirated-triple')
        self.assertTrue(len(matching_items) == 1)
        no_items = location_file_parser.get_context_matches(self.context, 'aspirated-single')
        self.assertTrue(len(no_items) == 0)
