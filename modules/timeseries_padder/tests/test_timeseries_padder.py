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
        self.out_path = Path('/tmp/out')
        self.pad_dirs = ['tmp']
        self.copy_dirs = ['copy_data1','copy_data2']
        self.non_copy_dir = 'non_copy'
        location = 'CFGLOC112154'
        input_root = Path('/tmp/in')
        month_path = Path('prt/2018/01')
        day = '03'
        self.input_path = Path(input_root, month_path)
        self.fs.create_dir(self.input_path)
        self.metadata_path = Path(month_path, day, location)
        #  data file
        self.data_filename = f'prt_{location}_2018-01-{day}.ext'
        data_path = Path(input_root, self.metadata_path, Config.data_dir, self.data_filename)
        self.fs.create_file(data_path)
        for i in self.copy_dirs:
            other_data_path = Path(input_root, self.metadata_path, i, self.data_filename)
            print(f'other_data_path: {other_data_path}')
            self.fs.create_file(other_data_path)
        non_copy_path = Path(input_root, self.metadata_path, self.non_copy_dir, self.data_filename)
        self.fs.create_file(non_copy_path)
        # config file (real file for parsing)
        this_path = Path(os.path.dirname(__file__))
        config_path = Path(this_path.parent, 'config/windowSizeNames.yaml')
        print(f'config_path: {config_path}')
        test_config = Path(this_path.parent, 'config/windowSizeNames.yaml')
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

    def test_constant_pad(self):
        config = Config(data_path=self.input_path,
                        out_path=self.out_path,
                        pad_dirs=self.pad_dirs,
                        copy_dirs=self.copy_dirs,
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

    def test_variable_pad(self):
        config = Config(data_path=self.input_path,
                        out_path=self.out_path,
                        pad_dirs=self.pad_dirs,
                        copy_dirs=self.copy_dirs,
                        relative_path_index=self.relative_path_index,
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
        not_exists_data_path = Path(self.out_path, 'prt/2018/01/01/CFGLOC112154/data', self.data_filename)
        previous_data_path = Path(self.out_path, 'prt/2018/01/02/CFGLOC112154/data', self.data_filename)
        next_data_path = Path(self.out_path, 'prt/2018/01/04/CFGLOC112154/data', self.data_filename)
        data_path = Path(self.out_path, self.metadata_path, Config.data_dir, self.data_filename)
        manifest_path = Path(self.out_path, self.metadata_path, Config.data_dir, Config.manifest_filename)
        threshold_path = Path(self.out_path, self.metadata_path, Config.threshold_dir, Config.threshold_filename)
        print(f'data_path: {data_path}')
        print(f'manifest_path: {manifest_path}')
        print(f'threshold_path: {threshold_path}')
        with open(str(manifest_path), 'r') as manifest_file:
            for line in manifest_file.readlines():
                print(f'manifest line: {line.strip()}')
        self.assertFalse(not_exists_data_path.exists())
        self.assertTrue(previous_data_path.exists())
        self.assertTrue(next_data_path.exists())
        self.assertTrue(data_path.exists())
        for i in self.copy_dirs:
            other_data_path = Path(self.out_path, self.metadata_path, i, self.data_filename)
            print(f'copy_path: {other_data_path}')
            self.assertTrue(other_data_path.exists())
        non_copy_path = Path(self.out_path, self.metadata_path, self.non_copy_dir, self.data_filename)
        self.assertFalse(non_copy_path.exists())
        self.assertTrue(manifest_path.exists())
        self.assertTrue(threshold_path.exists())
