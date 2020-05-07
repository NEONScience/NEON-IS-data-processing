#!/usr/bin/env python3
import os

from pyfakefs.fake_filesystem_unittest import TestCase

import data_calibration_group.app as app
from lib import log_config as log_config


class GrouperNoCalibrationTest(TestCase):

    def setUp(self):
        log_config.configure('DEBUG')

        self.setUpPyfakefs()

        self.data_metadata_path = os.path.join('prt', '2019', '07', '23', '0001')
        self.out_path = os.path.join('/', 'outputs')
        self.calibration_metadata_path = os.path.join('prt', '0001')

        self.data_filename = 'prt_0001_2018-01-03.ext'

        in_path = os.path.join('/', 'inputs')
        data_path = os.path.join(in_path, 'data', self.data_metadata_path, self.data_filename)
        calibration_path = os.path.join(in_path, 'calibration')

        self.fs.create_file(data_path)
        self.fs.create_file(calibration_path)

        self.data_path = os.path.join(in_path, 'data')
        self.calibration_path = os.path.join(in_path, 'calibration')

        self.data_source_type_index = '3'
        self.data_year_index = '4'
        self.data_month_index = '5'
        self.data_day_index = '6'
        self.calibration_source_type_index = '3'
        self.calibration_source_id_index = '4'
        self.calibration_stream_index = '5'

    def test_app(self):
        os.environ['DATA_PATH'] = self.data_path
        os.environ['CALIBRATION_PATH'] = self.calibration_path
        os.environ['OUT_PATH'] = self.out_path
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
        output_root = os.path.join(self.out_path, self.data_metadata_path)
        calibration_path = os.path.join(output_root, 'calibration')
        data_path = os.path.join(output_root, 'data', self.data_filename)
        self.assertTrue(os.path.lexists(calibration_path))
        self.assertTrue(os.path.lexists(data_path))
        self.assertFalse(os.path.lexists(os.path.join(calibration_path, self.calibration_metadata_path)))
