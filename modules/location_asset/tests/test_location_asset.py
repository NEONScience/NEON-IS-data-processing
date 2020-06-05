#!/usr/bin/env python3
import os
from pathlib import Path

import unittest

from pyfakefs.fake_filesystem_unittest import TestCase

import location_asset.app as app


class LocationAssetTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = Path('/output')
        self.fs.create_dir(self.out_path)
        #  Database URL in the form: [user]/[pass]@[url]:[port]/[sid]
        self.database_url = os.getenv('DATABASE_URL')

    @unittest.skip('Skip due to long process time.')
    def test_app(self):
        os.environ['DATABASE_URL'] = self.database_url
        os.environ['OUT_PATH'] = self.out_path
        os.environ['LOG_LEVEL'] = 'DEBUG'
        app.main()
        self.check_output()

    def check_output(self):
        expected_path = self.out_path.joinpath('prt/2201/prt_2201_locations.json')
        self.assertTrue(expected_path.exists())
