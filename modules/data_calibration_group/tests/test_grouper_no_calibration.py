#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

import data_calibration_group.app as app


class GrouperNoCalibrationTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()

        in_path = Path('/', 'inputs')
        self.out_path = Path('/', 'outputs')

        self.calibration_input_path = in_path.joinpath('calibration')
        self.data_input_path = in_path.joinpath('data')

        self.data_metadata_path = Path('prt', '2019', '07', '23', '0001')
        self.calibration_metadata_path = Path('prt', '0001')

        self.data_filename = 'prt_0001_2018-01-03.ext'

        self.fs.create_file(in_path.joinpath('data', self.data_metadata_path, self.data_filename))
        self.fs.create_dir(self.calibration_input_path.joinpath(self.calibration_metadata_path))

        self.data_source_type_index = '3'
        self.data_year_index = '4'
        self.data_month_index = '5'
        self.data_day_index = '6'
        self.calibration_source_type_index = '3'
        self.calibration_source_id_index = '4'
        self.calibration_stream_index = '5'

    def test_app(self):
        os.environ['DATA_PATH'] = str(self.data_input_path)
        os.environ['CALIBRATION_PATH'] = str(self.calibration_input_path)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['DATA_SOURCE_TYPE_INDEX'] = self.data_source_type_index
        os.environ['DATA_YEAR_INDEX'] = self.data_year_index
        os.environ['DATA_MONTH_INDEX'] = self.data_month_index
        os.environ['DATA_DAY_INDEX'] = self.data_day_index
        os.environ['CALIBRATION_SOURCE_TYPE_INDEX'] = self.calibration_source_type_index
        os.environ['CALIBRATION_SOURCE_ID_INDEX'] = self.calibration_source_id_index
        os.environ['CALIBRATION_STREAM_INDEX'] = self.calibration_stream_index
        app.main()
        self.check_output()

    def check_output(self):
        root_path = Path(self.out_path, self.data_metadata_path)
        calibration_path = root_path.joinpath('calibration')
        data_path = root_path.joinpath('data', self.data_filename)
        metadata_path = calibration_path.joinpath(self.calibration_metadata_path)
        self.assertTrue(calibration_path.exists())
        self.assertTrue(data_path.exists())
        self.assertFalse(metadata_path.exists())
