#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

from timeseries_padder.timeseries_padder.pad_config import PadConfig
from timeseries_padder.timeseries_padder.data_file_path import DataFilePath
from timeseries_padder.timeseries_padder.variable_window_pad import VariableWindowPad
from timeseries_padder.timeseries_padder.constant_window_pad import ConstantWindowPad


class TimeSeriesPadTest(TestCase):

    def setUp(self):
        """Set required files in mock filesystem."""
        self.setUpPyfakefs()
        self.location = 'CFGLOC112154'

        input_root = Path('/tmp/in')
        self.out_path = Path('/tmp/out')

        month_path = Path('prt/2018/01')
        day = '03'
        self.input_path = Path(input_root, month_path, day)
        self.fs.create_dir(self.input_path)

        self.metadata_path = Path(month_path, day, self.location)
        #  data file
        self.data_filename = f'prt_{self.location}_2018-01-{day}.ext'
        data_path = Path(input_root, self.metadata_path, PadConfig.data_dir, self.data_filename)
        self.fs.create_file(data_path)
        # config file (real file for parsing)
        config_path = Path('timeseries_padder/config/windowSizeNames.yaml')
        test_config = Path(os.path.dirname(__file__), '../config/windowSizeNames.yaml')
        self.fs.add_real_file(test_config, target_path=config_path)
        # location file (real file for parsing)
        location_path = Path(input_root, self.metadata_path, 'location/prt_40202_locations.json')
        test_locations = Path(os.path.dirname(__file__), 'test-locations.json')
        self.fs.add_real_file(test_locations, target_path=location_path)
        # threshold file (real file for parsing)
        threshold_path = Path(input_root, self.metadata_path, PadConfig.threshold_dir, PadConfig.threshold_filename)
        test_thresholds = Path(os.path.dirname(__file__), 'test-thresholds.json')
        self.fs.add_real_file(test_thresholds, target_path=threshold_path)
        # path indices
        self.relative_path_index = 3
        self.data_file_path = DataFilePath(year_index=4, month_index=5, day_index=6, location_index=7,
                                           data_type_index=8)

    def test_constant_window(self):
        window_size = 1
        constant_window_pad = ConstantWindowPad(data_path=self.input_path,
                                                out_path=self.out_path,
                                                relative_path_index=self.relative_path_index,
                                                window_size=window_size,
                                                data_file_path=self.data_file_path)
        constant_window_pad.pad()
        self.check_output()

    def test_variable_window(self):
        variable_pad = VariableWindowPad(self.input_path, self.out_path, self.data_file_path)
        variable_pad.pad()
        self.check_output()

    def check_output(self):
        """Ensure the expected files are in the output directory."""
        data_path = Path(self.out_path, self.metadata_path, PadConfig.data_dir, self.data_filename)
        manifest_path = Path(self.out_path, self.metadata_path, PadConfig.data_dir, PadConfig.manifest_filename)
        threshold_path = Path(self.out_path, self.metadata_path, PadConfig.threshold_dir, PadConfig.threshold_filename)
        self.assertTrue(data_path.exists())
        self.assertTrue(manifest_path.exists())
        self.assertTrue(threshold_path.exists())
