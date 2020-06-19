#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

import location_daily_linker.location_daily_linker_main as location_daily_linker_main
from location_daily_linker.location_file_path import LocationFilePath
from location_daily_linker.location_daily_linker import LocationDailyLinker


class LocationDailyLinkerTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.in_path = Path('/in/locations')
        self.out_path = Path('/out')
        self.location_path = Path(self.in_path, 'prt/2020/01/CFGLOC113836/location.json')
        # real location file for parsing
        real_location_path = Path(os.path.dirname(__file__), 'test-location.json')
        self.fs.add_real_file(real_location_path, target_path=self.location_path)
        # path indices
        self.source_type_index = 3
        self.year_index = 4
        self.month_index = 5
        self.location_index = 6

    def test_link_files(self):
        location_file_path = LocationFilePath(source_type_index=self.source_type_index,
                                              year_index=self.year_index,
                                              month_index=self.month_index,
                                              location_index=self.location_index)
        location_daily_linker = LocationDailyLinker(location_path=self.in_path, out_path=self.out_path,
                                                    location_file_path=location_file_path)
        location_daily_linker.link_files()
        self.check_output()

    def test_main(self):
        os.environ['LOCATION_PATH'] = str(self.in_path)
        os.environ['SOURCE_TYPE_INDEX'] = str(self.source_type_index)
        os.environ['YEAR_INDEX'] = str(self.year_index)
        os.environ['MONTH_INDEX'] = str(self.month_index)
        os.environ['LOCATION_INDEX'] = str(self.location_index)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        location_daily_linker_main.main()
        self.check_output()

    def check_output(self):
        expected_path_1 = Path(self.out_path, 'prt/2020/01/01/CFGLOC113836/location.json')
        expected_path_2 = Path(self.out_path, 'prt/2020/01/31/CFGLOC113836/location.json')
        self.assertTrue(expected_path_1.exists())
        self.assertTrue(expected_path_2.exists())
