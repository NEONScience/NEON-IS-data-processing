#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

import location_daily_linker.location_daily_linker_main as location_active_dates_main
from location_daily_linker.location_daily_linker import link_files


class LocationDailyLinkerTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = Path('/output')
        self.fs.create_dir(self.out_path)

        self.input_path = Path('/input/locations')
        self.location_path = self.input_path.joinpath('prt/CFGLOC113836/location.json')

        # Use real location file for parsing
        actual_location_file_path = Path(os.path.dirname(__file__), 'test-location.json')
        self.fs.add_real_file(actual_location_file_path, target_path=self.location_path)

        self.schema_index = 3

    def test_link_files(self):
        link_files(location_path=self.input_path, out_path=self.out_path, schema_index=self.schema_index)
        self.check_output()

    def test_main(self):
        os.environ['LOCATION_PATH'] = str(self.input_path)
        os.environ['SCHEMA_INDEX'] = str(self.schema_index)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        location_active_dates_main.main()
        self.check_output()

    def check_output(self):
        expected_input_path = Path(self.input_path, self.location_path)
        self.assertTrue(expected_input_path.exists())
        expected_path_1 = Path(self.out_path, 'prt/2020/06/30/CFGLOC113836/location.json')
        expected_path_2 = Path(self.out_path, 'prt/2020/07/01/CFGLOC113836/location.json')
        self.assertTrue(expected_path_1.exists())
        self.assertTrue(expected_path_2.exists())
