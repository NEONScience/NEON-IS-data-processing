#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

import location_active_dates.active_period_loader as active_period_loader


class ActivePeriodLoaderTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = Path('/', 'output')
        self.fs.create_dir(self.out_path)

    def test_active_period_loader(self):
        os.environ['LOCATION_PATH'] = 'CONFIG'
        os.environ['SCHEMA_INDEX'] = str(4)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        active_period_loader.main()
        self.check_output()

    def check_output(self):
        file_path = Path(self.out_path, 'prt', '2019', '10', '20', 'CFGLOC101740', 'CFGLOC101740.json')
        self.assertTrue(file_path.exists())
