#!/usr/bin/env python3
import os

import unittest
from pyfakefs.fake_filesystem_unittest import TestCase

from lib import log_config as log_config
import grouper.app as app


class AppTest(TestCase):

    def setUp(self):
        log_config.configure('DEBUG')
        self.setUpPyfakefs()

        self.input_path = os.path.join('/', 'repo', 'inputs')
        self.output_path = os.path.join('/', 'outputs')

        self.group = 'aspirated-single-121'

        self.location = 'CFGLOC123'

        self.metadata_path = os.path.join('2019', '05', '24', self.group)

        self.data_dir = 'data'
        self.location_dir = 'location'

        self.data_file = 'data.ext'
        self.location_file = 'locations.json'

        self.base_path = os.path.join(self.input_path, 'prt', self.metadata_path)

        self.in_data_path = os.path.join(self.base_path, self.location, self.data_dir, self.data_file)
        self.in_location_path = os.path.join(self.base_path, self.location, self.location_dir, self.location_file)

        self.fs.create_file(self.in_data_path)
        self.fs.create_file(self.in_location_path)

        self.relative_path_index = 3

    def test_group(self):
        app.group(self.base_path, self.output_path, self.relative_path_index)
        self.check_output()

    def test_main(self):
        os.environ['DATA_PATH'] = self.base_path
        os.environ['OUT_PATH'] = self.output_path
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['RELATIVE_PATH_INDEX'] = str(self.relative_path_index)
        app.main()
        self.check_output()

    def check_output(self):
        root_path = os.path.join(self.output_path, 'prt', self.metadata_path, self.location)

        out_data_path = os.path.join(root_path, self.data_dir, self.data_file)
        out_location_path = os.path.join(root_path, self.location_dir, self.location_file)

        print(f'in_data_path: {self.in_data_path}')
        print(f'in_location_path: {self.in_location_path}')

        print(f'out_data_path: {out_data_path}')
        print(f'out_location_path: {out_location_path}')

        self.assertTrue(os.path.lexists(out_data_path))
        self.assertTrue(os.path.lexists(out_location_path))


if __name__ == '__main__':
    unittest.main()
