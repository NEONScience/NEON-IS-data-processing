#!/usr/bin/env python3
import os

from pyfakefs.fake_filesystem_unittest import TestCase

import location_group_path.app as app
from lib import log_config as log_config


class AppTest(TestCase):

    def setUp(self):
        log_config.configure('DEBUG')

        self.location = 'CFGLOC113507'
        # The context group to find in the location file should match the existing file entry 'aspirated-single-224'.
        self.group = 'aspirated-triple-'

        self.setUpPyfakefs()

        self.in_path = os.path.join('/', 'inputs')
        self.out_path = os.path.join('/', 'outputs')

        self.metadata_path = os.path.join('dualfan', '2019', '05', '21')

        inputs_root = os.path.join(self.in_path, 'repo', self.metadata_path)

        data_path = os.path.join(inputs_root, self.location, 'data', 'data.ext')
        locations_path = os.path.join(inputs_root, self.location, 'location', 'locations.json')

        self.fs.create_file(data_path)

        # Use real location file for parsing
        actual_location_file_path = os.path.join(os.path.dirname(__file__), 'test-locations.json')
        self.fs.add_real_file(actual_location_file_path, target_path=locations_path)

    def test_main(self):
        os.environ['SOURCE_PATH'] = self.in_path
        os.environ['OUT_PATH'] = self.out_path
        os.environ['GROUP'] = self.group
        os.environ['LOG_LEVEL'] = 'DEBUG'
        app.main()
        self.check_output()

    def check_output(self):

        root_path_1 = os.path.join(self.out_path, self.metadata_path, 'aspirated-triple-224', self.location)
        root_path_2 = os.path.join(self.out_path, self.metadata_path, 'aspirated-triple-226', self.location)

        data_path_1 = os.path.join(root_path_1, 'data', 'data.ext')
        locations_path_1 = os.path.join(root_path_2, 'location', 'locations.json')

        data_path_2 = os.path.join(root_path_1, 'data', 'data.ext')
        locations_path_2 = os.path.join(root_path_2, 'location', 'locations.json')

        print(f'data_path_1: {data_path_1}')
        print(f'locations_path_1: {locations_path_1}')

        check_path_1 = os.path.join(self.out_path, 'dualfan', '2019', '05', '21', 'aspirated-triple-224', self.location)
        print(os.listdir(check_path_1))
        check_path_2 = os.path.join(self.out_path, 'dualfan', '2019', '05', '21', 'aspirated-triple-226', self.location)
        print(os.listdir(check_path_2))

        self.assertTrue(os.path.lexists(data_path_1))
        self.assertTrue(os.path.lexists(locations_path_1))
        self.assertTrue(os.path.lexists(data_path_2))
        self.assertTrue(os.path.lexists(locations_path_2))
