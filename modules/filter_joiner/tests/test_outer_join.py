#!/usr/bin/env python3
import os
import yaml
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

from filter_joiner.joiner import FilterJoiner
import filter_joiner.filter_joiner_main as filter_joiner_main


class OuterJoinTest(TestCase):

    def setUp(self):
        """Create files to join in fake filesystem."""
        self.config_file = 'config-outer-join.yaml'
        self.config_file_target_path = f'/{self.config_file}'
        self.setUpPyfakefs()
        self.input_path = Path('/in')
        self.output_path = Path('/out')
        # This path will not be joined on 'dir3'.
        self.path_1 = Path('dir1/dir3/extra/long/path/file_1.txt')
        # This path will not be joined because of above path.
        self.path_2 = Path('dir1/dir2/extra/long/path/file_2.txt')
        # This path will determine the other joined paths since it contains outer_join: true in the config.
        self.path_3 = Path('dir1/dir2/extra/long/path/file_3.txt')
        self.input_path_1 = Path(self.input_path, 'INPUT_1', self.path_1)
        self.input_path_2 = Path(self.input_path, 'INPUT_2', self.path_2)
        self.input_path_3 = Path(self.input_path, 'INPUT_3', self.path_3)
        self.fs.create_file(self.input_path_1)
        self.fs.create_file(self.input_path_2)
        self.fs.create_file(self.input_path_3)
        # Use real config file
        config_file_path = Path(os.path.dirname(__file__), self.config_file)
        self.fs.add_real_file(config_file_path, target_path=self.config_file_target_path)
        self.relative_path_index = 3

    def test_joiner(self):
        with open(self.config_file_target_path) as f:
            data = yaml.load(f, Loader=yaml.FullLoader)
            config = yaml.dump(data, sort_keys=True)
        filter_joiner = FilterJoiner(config=config,
                                     out_path=self.output_path,
                                     relative_path_index=self.relative_path_index)
        filter_joiner.join()
        self.check_output()

    def test_main(self):
        with open(self.config_file_target_path) as f:
            data = yaml.load(f, Loader=yaml.FullLoader)
            config = yaml.dump(data, sort_keys=True)
        os.environ['CONFIG'] = config
        os.environ['OUT_PATH'] = str(self.output_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['RELATIVE_PATH_INDEX'] = str(self.relative_path_index)
        filter_joiner_main.main()
        self.check_output()

    def check_output(self):
        path_1 = Path(self.output_path, self.path_1)
        path_2 = Path(self.output_path, self.path_2)
        path_3 = Path(self.output_path, self.path_3)
        self.assertFalse(path_1.exists())
        self.assertFalse(path_2.exists())
        self.assertTrue(path_3.exists())
