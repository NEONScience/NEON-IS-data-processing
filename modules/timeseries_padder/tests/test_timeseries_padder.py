#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

from timeseries_padder.timeseries_padder.timeseries_padder_config import Config
from timeseries_padder.timeseries_padder.variable_pad import VariablePad
from timeseries_padder.timeseries_padder.constant_pad import ConstantPad


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
        data_path = Path(input_root, self.metadata_path, Config.data_dir, self.data_filename)
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
        threshold_path = Path(input_root, self.metadata_path, Config.threshold_dir, Config.threshold_filename)
        test_thresholds = Path(os.path.dirname(__file__), 'test-thresholds.json')
        self.fs.add_real_file(test_thresholds, target_path=threshold_path)
        # path indices
        self.relative_path_index = 3
        self.year_index = 4
        self.month_index = 5
        self.day_index = 6
        self.location_index = 7
        self.data_type_index = 8

    def test_constant_window(self):
        config = Config(data_path=self.input_path,
                        out_path=self.out_path,
                        relative_path_index=self.relative_path_index,
                        year_index=self.year_index,
                        month_index=self.month_index,
                        day_index=self.day_index,
                        location_index=self.location_index,
                        data_type_index=self.data_type_index,
                        window_size=1)
        constant_pad = ConstantPad(config)
        constant_pad.pad()
        self.check_output()

    def test_variable_window(self):
        config = Config(data_path=self.input_path,
                        out_path=self.out_path,
                        relative_path_index=0,
                        year_index=self.year_index,
                        month_index=self.month_index,
                        day_index=self.day_index,
                        location_index=self.location_index,
                        data_type_index=self.data_type_index,
                        window_size=0)
        variable_pad = VariablePad(config)
        variable_pad.pad()
        self.check_output()

    def check_output(self):
        """Ensure the expected files are in the output directory."""
        data_path = Path(self.out_path, self.metadata_path, Config.data_dir, self.data_filename)
        manifest_path = Path(self.out_path, self.metadata_path, Config.data_dir, Config.manifest_filename)
        threshold_path = Path(self.out_path, self.metadata_path, Config.threshold_dir, Config.threshold_filename)
        self.assertTrue(data_path.exists())
        self.assertTrue(manifest_path.exists())
        self.assertTrue(threshold_path.exists())
