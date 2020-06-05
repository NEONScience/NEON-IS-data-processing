#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

import location_group_path.app as app
from common import log_config as log_config


class LocationGroupPathTest(TestCase):

    def setUp(self):
        log_config.configure('DEBUG')

        self.location = 'CFGLOC113507'
        # The context group to find in the location file should contains_match the existing file entry 'aspirated-single-224'.
        self.group = 'aspirated-triple-'

        self.setUpPyfakefs()

        self.in_path = Path('/', 'inputs')
        self.out_path = Path('/', 'outputs')

        self.metadata_path = Path('dualfan', '2019', '05', '21')

        inputs_root = Path(self.in_path, 'repo', self.metadata_path)

        data_path = Path(inputs_root, self.location, 'data', 'data.ext')
        locations_path = Path(inputs_root, self.location, 'location', 'locations.json')

        self.fs.create_file(data_path)

        # Use real location file for parsing
        actual_location_file_path = Path(os.path.dirname(__file__), 'test-locations.json')
        self.fs.add_real_file(actual_location_file_path, target_path=locations_path)

        self.source_type_index = 3
        self.year_index = 4
        self.month_index = 5
        self.day_index = 6
        self.location_index = 7
        self.data_type_index = 8

    def test_main(self):
        os.environ['SOURCE_PATH'] = str(self.in_path)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['GROUP'] = self.group
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['SOURCE_TYPE_INDEX'] = str(self.source_type_index)
        os.environ['YEAR_INDEX'] = str(self.year_index)
        os.environ['MONTH_INDEX'] = str(self.month_index)
        os.environ['DAY_INDEX'] = str(self.day_index)
        os.environ['LOCATION_INDEX'] = str(self.location_index)
        os.environ['DATA_TYPE_INDEX'] = str(self.data_type_index)
        app.main()
        self.check_output()

    def check_output(self):

        root_path_1 = Path(self.out_path, self.metadata_path, 'aspirated-triple-224', self.location)
        root_path_2 = Path(self.out_path, self.metadata_path, 'aspirated-triple-226', self.location)

        data_path_1 = Path(root_path_1, 'data', 'data.ext')
        locations_path_1 = Path(root_path_2, 'location', 'locations.json')

        data_path_2 = Path(root_path_1, 'data', 'data.ext')
        locations_path_2 = Path(root_path_2, 'location', 'locations.json')

        print(f'data_path_1: {data_path_1}')
        print(f'locations_path_1: {locations_path_1}')

        check_path_1 = Path(self.out_path, 'dualfan', '2019', '05', '21', 'aspirated-triple-224', self.location)
        print(os.listdir(check_path_1))
        check_path_2 = Path(self.out_path, 'dualfan', '2019', '05', '21', 'aspirated-triple-226', self.location)
        print(os.listdir(check_path_2))

        self.assertTrue(data_path_1.exists())
        self.assertTrue(locations_path_1.exists())
        self.assertTrue(data_path_2.exists())
        self.assertTrue(locations_path_2.exists())
