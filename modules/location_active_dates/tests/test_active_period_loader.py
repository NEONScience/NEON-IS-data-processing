#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

import location_active_dates.active_period_loader as active_period_loader


class ActivePeriodLoaderTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = Path('/output')
        self.fs.create_dir(self.out_path)

        self.input_path = Path('/input/repo')
        self.location_path = self.input_path.joinpath('prt/CFGLOC113836/location.json')

        # Use real location file for parsing
        actual_location_file_path = Path(os.path.dirname(__file__), 'test-location.json')
        self.fs.add_real_file(actual_location_file_path, target_path=self.location_path)

    def test_active_period_loader(self):
        os.environ['LOCATION_PATH'] = str(self.input_path)
        os.environ['SCHEMA_INDEX'] = str(3)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        active_period_loader.main()
        self.check_output()

    def check_output(self):
        expected_input_path = Path(self.input_path.joinpath(self.location_path))
        self.assertTrue(expected_input_path.exists())
        expected_path_1 = self.out_path.joinpath('prt/2020/06/CFGLOC113836/location.json')
        expected_path_2 = self.out_path.joinpath('prt/2020/07/CFGLOC113836/location.json')
        self.assertTrue(expected_path_1.exists())
        self.assertTrue(expected_path_2.exists())
