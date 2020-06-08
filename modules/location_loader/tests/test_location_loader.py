#!/usr/bin/env python3
import os
from pathlib import Path

import unittest

from pyfakefs.fake_filesystem_unittest import TestCase

import location_loader.location_loader as location_loader


class LocationActiveDatesTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = Path('/', 'output')
        self.fs.create_dir(self.out_path)
        #  Database URL in the form: [user]/[pass]@[url]:[port]/[sid]
        self.database_url = os.getenv('DATABASE_URL')

    @unittest.skip('Skip due to long process time.')
    def test_location_file_loader(self):
        os.environ['LOCATION_TYPE'] = 'CONFIG'
        os.environ['DATABASE_URL'] = self.database_url
        os.environ['tick'] = '/pfs/tick/2015-11-01T00:00:00Z'
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        location_loader.main()
        self.check_output()

    def check_output(self):
        file_path = Path(self.out_path, 'prt/2019/10/20/CFGLOC101740/CFGLOC101740.json')
        self.assertTrue(file_path.exists())
