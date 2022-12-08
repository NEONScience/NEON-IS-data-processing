#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

import location_group_path.location_group_path_main as location_group_path_main
from location_group_path.location_group_path_config import Config
from location_group_path.location_group_path import LocationGroupPath


class LocationGroupPathTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.location = 'CFGLOC113507'
        # The context group to find in the test location file (it contains 'aspirated-single-224').
        self.group = 'aspirated-triple-'
        self.source_path = Path('/in')
        self.out_path = Path('/out')
        self.source_type = 'dualfan'
        self.date_path = Path('2019/05/21')
        inputs_root = Path(self.source_path, 'repo', self.source_type, self.date_path)
        data_path = Path(inputs_root, self.location, 'data/data.ext')
        locations_path = Path(inputs_root, self.location, 'location/locations.json')
        self.fs.create_file(data_path)
        # use real location file for parsing
        actual_location_file_path = Path(os.path.dirname(__file__), 'test-locations.json')
        self.fs.add_real_file(actual_location_file_path, target_path=locations_path)
        # path indices
        self.source_type_index = 3
        self.year_index = 4
        self.month_index = 5
        self.day_index = 6
        self.location_index = 7
        self.data_type_index = 8

    def test_add_groups_to_paths(self):
        config = Config(source_path=self.source_path,
                        out_path=self.out_path,
                        group=self.group,
                        source_type_index=self.source_type_index,
                        year_index=self.year_index,
                        month_index=self.month_index,
                        day_index=self.day_index,
                        location_index=self.location_index,
                        data_type_index=self.data_type_index)
        location_group_path = LocationGroupPath(config)
        location_group_path.add_groups_to_paths()
        self.check_output()

    def test_main(self):
        os.environ['SOURCE_PATH'] = str(self.source_path)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['GROUP'] = self.group
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['SOURCE_TYPE_INDEX'] = str(self.source_type_index)
        os.environ['YEAR_INDEX'] = str(self.year_index)
        os.environ['MONTH_INDEX'] = str(self.month_index)
        os.environ['DAY_INDEX'] = str(self.day_index)
        os.environ['LOCATION_INDEX'] = str(self.location_index)
        os.environ['DATA_TYPE_INDEX'] = str(self.data_type_index)
        location_group_path_main.main()
        self.check_output()

    def check_output(self):
        root_path_1 = Path(self.out_path, self.date_path, 'aspirated-triple-224', self.source_type, self.location)
        root_path_2 = Path(self.out_path, self.date_path, 'aspirated-triple-226', self.source_type, self.location)
        data_path_1 = Path(root_path_1, 'data/data.ext')
        locations_path_1 = Path(root_path_2, 'location/locations.json')
        data_path_2 = Path(root_path_1, 'data/data.ext')
        locations_path_2 = Path(root_path_2, 'location/locations.json')
        self.assertTrue(data_path_1.exists())
        self.assertTrue(locations_path_1.exists())
        self.assertTrue(data_path_2.exists())
        self.assertTrue(locations_path_2.exists())
