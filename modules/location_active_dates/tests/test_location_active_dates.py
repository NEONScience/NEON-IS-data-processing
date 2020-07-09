#!/usr/bin/env python3
import os
from pathlib import Path
from datetime import date

from pyfakefs.fake_filesystem_unittest import TestCase

import location_active_dates.location_active_dates_main as location_active_dates_main
from location_active_dates.location_active_dates import link_location_files, dates_between


class LocationActiveDatesTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = Path('/out')
        self.fs.create_dir(self.out_path)
        self.input_path = Path('/in/repo')
        self.location_path = self.input_path.joinpath('prt/CFGLOC113836/location.json')
        # Use real location file for parsing
        actual_location_file_path = Path(os.path.dirname(__file__), 'test-location.json')
        self.fs.add_real_file(actual_location_file_path, target_path=self.location_path)
        self.schema_index = 3

    def test_dates_between(self):
        start_date = date(2019, 2, 24)
        end_date = date(2019, 3, 3)
        dates = []
        for returned_date in dates_between(start_date, end_date):
            dates.append(returned_date)
        self.assertEqual(len(dates), 8)

    def test_link_location_files(self):
        link_location_files(location_path=self.input_path, out_path=self.out_path, schema_index=self.schema_index)
        self.check_output()

    def test_main(self):
        os.environ['LOCATION_PATH'] = str(self.input_path)
        os.environ['SCHEMA_INDEX'] = str(self.schema_index)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        location_active_dates_main.main()
        self.check_output()

    def check_output(self):
        expected_input_path = Path(self.input_path.joinpath(self.location_path))
        self.assertTrue(expected_input_path.exists())
        expected_path_1 = self.out_path.joinpath('prt/2020/06/CFGLOC113836/location.json')
        expected_path_2 = self.out_path.joinpath('prt/2020/07/CFGLOC113836/location.json')
        self.assertTrue(expected_path_1.exists())
        self.assertTrue(expected_path_2.exists())
