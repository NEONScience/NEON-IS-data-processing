#!/usr/bin/env python3
import os
import yaml
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

from filter_joiner.joiner import FilterJoiner
import filter_joiner.filter_joiner_main as filter_joiner_main


class FilterJoinerLocationGroupTest(TestCase):
    """Class to simulate pipeline location/data join on source ID."""

    def setUp(self):
        """Create files to join in fake filesystem."""
        self.setUpPyfakefs()
        self.input_path = Path('/in')
        self.output_path = Path('/out')
        self.path_1 = Path('prt/1234/prt_1234_locations.json')
        self.path_2 = Path('prt/2020/01/02/1234/prt_1234_2020-01-02.parquet')
        self.input_path_1 = Path(self.input_path, 'location', self.path_1)
        self.input_path_2 = Path(self.input_path, 'data', self.path_2)
        self.fs.create_file(self.input_path_1)
        self.fs.create_file(self.input_path_2)
        # Use real config file
        config_file_path = Path(os.path.dirname(__file__), 'config-location-group.yaml')
        self.fs.add_real_file(config_file_path, target_path='/config.yaml')
        self.relative_path_index = 3

    def test_joiner(self):
        with open('/config.yaml') as f:
            data = yaml.load(f, Loader=yaml.FullLoader)
            config = yaml.dump(data, sort_keys=True)
        filter_joiner = FilterJoiner(config=config,
                                     out_path=self.output_path,
                                     relative_path_index=self.relative_path_index)
        filter_joiner.join()
        self.check_output()

    def test_main(self):
        with open('/config.yaml') as f:
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
        self.assertTrue(path_1.exists())
        self.assertTrue(path_2.exists())
