#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

import lib.log_config as log_config
from lib.merged_data_filename import MergedDataFilename
from timeseries_padder.timeseries_padder.padder import Padder
from timeseries_padder.timeseries_padder.constant_padder import ConstantPadder


class TimeSeriesPadTest(TestCase):

    def setUp(self):
        """Set required files in mock filesystem."""

        log_config.configure('DEBUG')

        self.threshold_dir = 'threshold'

        self.out_dir = Path('/', 'tmp', 'outputs')
        self.location = 'CFGLOC112154'
        self.input_root = Path('/', 'tmp', 'inputs')

        source_month = Path('prt', '2018', '01')
        self.input_data_dir = Path(self.input_root, source_month, '03')
        self.source_dir = Path(source_month, '03', self.location)

        self.relative_path_index = 3
        self.year_index = 4
        self.month_index = 5
        self.day_index = 6
        self.location_index = 7
        self.data_type_index = 8

        self.setUpPyfakefs()

        self.data_dir = 'data'

        #  Data file.
        self.source_data_file_name = MergedDataFilename.build('prt', '2018', '01', '03', self.location)
        data_path = Path(self.input_root, self.source_dir, self.data_dir, self.source_data_file_name)
        self.fs.create_file(data_path)

        # Config file (real file for parsing)
        config_path = Path('timeseries_padder', 'config', 'windowSizeNames.yaml')
        test_config = Path(os.path.dirname(__file__), '..', 'config', 'windowSizeNames.yaml')
        self.fs.add_real_file(test_config, target_path=config_path)

        # Location file (real file for parsing)
        location_path = Path(self.input_root, self.source_dir, 'location', 'prt_40202_locations.json')
        test_locations = Path(os.path.dirname(__file__), 'test-locations.json')
        self.fs.add_real_file(test_locations, target_path=location_path)

        # Threshold file (real file for parsing)
        threshold_path = Path(self.input_root, self.source_dir, self.threshold_dir, 'thresholds.json')
        test_thresholds = Path(os.path.dirname(__file__), 'test-thresholds.json')
        self.fs.add_real_file(test_thresholds, target_path=threshold_path)

    def test_constant_padder(self):
        window_size = 1
        padder = ConstantPadder(self.input_data_dir, self.out_dir, self.year_index, self.month_index,
                             self.day_index, self.location_index, self.data_type_index, self.relative_path_index,
                             window_size)
        padder.pad()
        self.check_output()

    def test_padder(self):
        padder = Padder(self.input_data_dir, self.out_dir, self.year_index, self.month_index,
                        self.day_index, self.location_index, self.data_type_index)
        padder.pad()
        self.check_output()

    def check_output(self):
        """Check data files are in the output directory."""
        data_path = Path(self.out_dir, self.source_dir, self.data_dir, self.source_data_file_name)
        manifest_path = Path(self.out_dir, self.source_dir, self.data_dir, 'manifest.txt')
        threshold_path = Path(self.out_dir, self.source_dir, self.threshold_dir, 'thresholds.json')
        self.assertTrue(data_path.exists())
        self.assertTrue(manifest_path.exists())
        self.assertTrue(threshold_path.exists())
