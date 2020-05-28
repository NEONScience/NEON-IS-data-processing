#!/usr/bin/env python3
import os
import yaml

import unittest
from pyfakefs.fake_filesystem_unittest import TestCase

import file_joiner.joiner as joiner


class FileJoinerTest(TestCase):

    def setUp(self):
        """Create files to join in fake filesystem."""
        self.setUpPyfakefs()

        self.input_path = os.path.join('/', 'pfs')
        self.output_path = os.path.join('/', 'outputs')

        self.path_1 = os.path.join('dir1', 'dir2', 'file_1.txt')
        self.path_2 = os.path.join('dir1', 'dir2', 'file_2.txt')
        self.path_3 = os.path.join('dir1', 'dir2', 'file_3.txt')

        self.input_path_1 = os.path.join(self.input_path, 'INPUT_1', self.path_1)
        self.input_path_2 = os.path.join(self.input_path, 'INPUT_2', self.path_2)
        self.input_path_3 = os.path.join(self.input_path, 'INPUT_3', self.path_3)

        self.fs.create_file(self.input_path_1)
        self.fs.create_file(self.input_path_2)
        self.fs.create_file(self.input_path_3)

        # Use real config file
        config_file_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
        self.fs.add_real_file(config_file_path, target_path='/config.yaml')

    def test_main(self):
        with open('/config.yaml') as f:
            data = yaml.load(f, Loader=yaml.FullLoader)
            config = yaml.dump(data, sort_keys=True)
        os.environ['CONFIG'] = config
        os.environ['OUT_PATH'] = self.output_path
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['RELATIVE_PATH_INDEX'] = '3'
        joiner.main()
        self.check_output()

    def check_output(self):
        path_1 = os.path.join(self.output_path, self.path_1)
        # Test output indices in config file with path_1
        # path_1 = os.path.join(self.output_path, 'dir2', 'file_1.txt')
        path_2 = os.path.join(self.output_path, self.path_2)
        path_3 = os.path.join(self.output_path, self.path_3)
        self.assertTrue(os.path.lexists(path_1))
        self.assertTrue(os.path.lexists(path_2))
        self.assertTrue(os.path.lexists(path_3))


if __name__ == '__main__':
    unittest.main()
