#!/usr/bin/env python3
import os

from pyfakefs.fake_filesystem_unittest import TestCase

import data_calibration_group.app as app
from lib import log_config as log_config


class GrouperCalibrationTest(TestCase):

    def setUp(self):
        log_config.configure('DEBUG')

        self.setUpPyfakefs()

        self.data_metadata_path = os.path.join('prt', '2019', '07', '23', '0001')
        self.out_path = os.path.join('/', 'outputs')
        self.calibration_metadata_path = os.path.join('prt', '0001')

        self.data_filename = 'prt_0001_2018-01-03.ext'

        #  Set input files in mock filesystem.
        in_path = os.path.join('/', 'inputs')
        data_path = os.path.join(in_path, 'data', self.data_metadata_path)
        calibration_path = os.path.join(in_path, 'calibration', self.calibration_metadata_path)
        resistance_input_dir = os.path.join(calibration_path, 'resistance')
        temperature_input_dir = os.path.join(calibration_path, 'temperature')

        #  Calibration files
        self.fs.create_file(os.path.join(resistance_input_dir, 'calibration1.xml'))
        self.fs.create_file(os.path.join(resistance_input_dir, 'calibration2.xml'))
        self.fs.create_file(os.path.join(temperature_input_dir, 'calibration1.xml'))
        self.fs.create_file(os.path.join(temperature_input_dir, 'calibration2.xml'))

        #  Data file
        self.fs.create_file(os.path.join(data_path, self.data_filename))

        self.data_path = os.path.join(in_path, 'data')
        self.calibration_path = os.path.join(in_path, 'calibration')

        self.data_source_type_index = '3'
        self.data_year_index = '4'
        self.data_month_index = '5'
        self.data_day_index = '6'
        self.calibration_source_type_index = '3'
        self.calibration_source_id_index = '4'
        self.calibration_stream_index = '5'

    def test_main(self):
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
        resistance_path = os.path.join(calibration_path, 'resistance')
        temperature_path = os.path.join(calibration_path, 'temperature')
        resistance_calibration1 = os.path.join(resistance_path, 'calibration1.xml')
        resistance_calibration2 = os.path.join(resistance_path, 'calibration2.xml')
        temperature_calibration1 = os.path.join(temperature_path, 'calibration1.xml')
        temperature_calibration2 = os.path.join(temperature_path, 'calibration2.xml')
        data_path = os.path.join(output_root, 'data', self.data_filename)

        self.assertTrue(os.path.lexists(resistance_calibration1))
        self.assertTrue(os.path.lexists(resistance_calibration2))
        self.assertTrue(os.path.lexists(temperature_calibration1))
        self.assertTrue(os.path.lexists(temperature_calibration2))
        self.assertTrue(os.path.lexists(data_path))
