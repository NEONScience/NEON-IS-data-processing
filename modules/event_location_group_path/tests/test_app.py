#!/usr/bin/env python3
import os

from pyfakefs.fake_filesystem_unittest import TestCase

import event_location_group_path.app as app
from lib import log_config as log_config


class AppTest(TestCase):

    def setUp(self):
        log_config.configure('DEBUG')

        self.location = 'CFGLOC113507'

        # The context group to find in the test location file entry 'aspirated-single-224'.
        self.group = 'aspirated-triple-'

        self.setUpPyfakefs()

        self.source_id = '00001'

        self.in_path = os.path.join('/', 'inputs')
        self.out_path = os.path.join('/', 'outputs')

        input_root = os.path.join(self.in_path, 'repo', 'heater')

        self.event_file = 'heater_' + self.source_id + '_events.json'
        self.location_file = 'heater_' + self.source_id + '_locations.json'

        data_path = os.path.join(input_root, self.source_id, 'data', self.event_file)
        locations_path = os.path.join(input_root, self.source_id, 'location', self.location_file)

        self.fs.create_file(data_path)

        # Use real location file for parsing
        actual_location_file_path = os.path.join(os.path.dirname(__file__), 'test-locations.json')
        self.fs.add_real_file(actual_location_file_path, target_path=locations_path)

        self.source_type_index = 3
        self.source_id_index = 4
        self.data_type_index = 5
        self.filename_index = 6

    def test_main(self):
        os.environ['SOURCE_PATH'] = self.in_path
        os.environ['OUT_PATH'] = self.out_path
        os.environ['GROUP'] = self.group
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['SOURCE_TYPE_INDEX'] = str(self.source_type_index)
        os.environ['SOURCE_ID_INDEX'] = str(self.source_id_index)
        os.environ['DATA_TYPE_INDEX'] = str(self.data_type_index)
        os.environ['FILENAME_INDEX'] = str(self.filename_index)
        app.main()
        self.check_output()

    def check_output(self):

        root_path = os.path.join(self.out_path, 'heater', 'aspirated-triple-224', self.source_id)

        data_path = os.path.join(root_path, 'data', self.event_file)
        location_path = os.path.join(root_path, 'location', self.location_file)

        print(f'data_path: {data_path}')
        print(f'location_path: {location_path}')

        check_path = os.path.join(self.out_path, 'heater', 'aspirated-triple-224', self.source_id)
        print(os.listdir(check_path))

        self.assertTrue(os.path.lexists(data_path))
        self.assertTrue(os.path.lexists(location_path))
