#!/usr/bin/env python3
import os

from pyfakefs.fake_filesystem_unittest import TestCase

import lib.log_config as log_config
from lib.merged_data_filename import MergedDataFilename
from timeseries_padder.timeseries_padder.padder import Padder
from timeseries_padder.timeseries_padder.constant_padder import ConstantPadder


class TimeseriesPadderTest(TestCase):

    def setUp(self):
        """Set required files in mock filesystem."""

        self.threshold_dir = 'threshold'

        log_config.configure('DEBUG')

        self.out_dir = os.path.join('/', 'tmp', 'outputs')
        self.location = 'CFGLOC112154'
        self.input_root = os.path.join('/', 'tmp', 'inputs',)

        source_month = os.path.join('prt', '2018', '01')
        self.input_data_dir = os.path.join(self.input_root, source_month, '03')
        self.source_dir = os.path.join(source_month, '03', self.location)

        self.relative_path_index = 3
        self.year_index = 4
        self.month_index = 5
        self.day_index = 6
        self.loc_index = 7
        self.subdir_index = 8

        self.setUpPyfakefs()

        self.data_dir = 'data'

        # Config file (a real file for parsing).
        config_path = os.path.join('timeseries_padder', 'config', 'windowSizeNames.yaml')
        test_config = os.path.join(os.path.dirname(__file__), '..', 'config', 'windowSizeNames.yaml')
        self.fs.add_real_file(test_config, target_path=config_path)

        #  Data file.
        self.source_data_file_name = MergedDataFilename.build('prt', '2018', '01', '03', self.location)
        data_path = os.path.join(self.input_root, self.source_dir, self.data_dir, self.source_data_file_name)
        self.fs.create_file(data_path)

        #  Location file (a real file for parsing).
        location_path = os.path.join(self.input_root, self.source_dir, 'location', 'prt_40202_locations.json')
        test_locations = os.path.join(os.path.dirname(__file__), 'test-locations.json')
        self.fs.add_real_file(test_locations, target_path=location_path)

        #  Threshold file (a real file for parsing).
        threshold_path = os.path.join(self.input_root, self.source_dir, self.threshold_dir, 'thresholds.json')
        test_thresholds = os.path.join(os.path.dirname(__file__), 'test-thresholds.json')
        self.fs.add_real_file(test_thresholds, target_path=threshold_path)

        print('Time series Padder Test')
        print(f'input data_path: {data_path}')
        print(f'input location path: {location_path}')
        print(f'input threshold path: {threshold_path}')

    def test_constant_padder(self):
        window_size = 1
        padder = ConstantPadder(self.input_data_dir, self.out_dir, self.year_index, self.month_index,
                                self.day_index, self.loc_index, self.subdir_index, self.relative_path_index,
                                window_size)
        padder.pad()
        self.check_output()

    def test_padder(self):
        padder = Padder(self.input_data_dir, self.out_dir, self.year_index, self.month_index,
                        self.day_index, self.loc_index, self.subdir_index)
        padder.pad()
        self.check_output()

    def check_output(self):
        """Check data files are in the output directory."""
        data_path = os.path.join(self.out_dir, self.source_dir, self.data_dir, self.source_data_file_name)
        manifest_path = os.path.join(self.out_dir, self.source_dir, self.data_dir, 'manifest.txt')
        threshold_path = os.path.join(self.out_dir, self.source_dir, self.threshold_dir, 'thresholds.json')

        print(f'data_path: {data_path}')
        print(f'manifest_path: {manifest_path}')
        print(f'threshold_path: {threshold_path}')

        self.assertTrue(os.path.lexists(data_path))
        self.assertTrue(os.path.exists(manifest_path))
        self.assertTrue(os.path.lexists(threshold_path))
