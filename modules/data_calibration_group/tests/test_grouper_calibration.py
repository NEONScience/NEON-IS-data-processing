#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

import data_calibration_group.app as app


class GrouperCalibrationTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()

        self.data_metadata_path = Path('prt', '2019', '07', '23', '0001')
        self.out_path = Path('/', 'outputs')
        self.calibration_metadata_path = Path('prt', '0001')

        self.data_filename = 'prt_0001_2018-01-03.ext'

        #  Set input files in mock filesystem.
        in_path = Path('/', 'inputs')
        data_path = in_path.joinpath('data', self.data_metadata_path)
        calibration_path = in_path.joinpath('calibration', self.calibration_metadata_path)
        resistance_input_dir = calibration_path.joinpath('resistance')
        temperature_input_dir = calibration_path.joinpath('temperature')

        #  Calibration files
        self.fs.create_file(resistance_input_dir.joinpath('calibration1.xml'))
        self.fs.create_file(resistance_input_dir.joinpath('calibration2.xml'))
        self.fs.create_file(temperature_input_dir.joinpath('calibration1.xml'))
        self.fs.create_file(temperature_input_dir.joinpath('calibration2.xml'))

        #  Data file
        self.fs.create_file(data_path.joinpath(self.data_filename))

        self.data_path = in_path.joinpath('data')
        self.calibration_path = in_path.joinpath('calibration')

        self.data_source_type_index = '3'
        self.data_year_index = '4'
        self.data_month_index = '5'
        self.data_day_index = '6'
        self.calibration_source_type_index = '3'
        self.calibration_source_id_index = '4'
        self.calibration_stream_index = '5'

    def test_main(self):
        os.environ['DATA_PATH'] = str(self.data_path)
        os.environ['CALIBRATION_PATH'] = str(self.calibration_path)
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
        resistance_path = calibration_path.joinpath('resistance')
        temperature_path = calibration_path.joinpath('temperature')
        resistance_calibration1 = resistance_path.joinpath('calibration1.xml')
        resistance_calibration2 = resistance_path.joinpath('calibration2.xml')
        temperature_calibration1 = temperature_path.joinpath('calibration1.xml')
        temperature_calibration2 = temperature_path.joinpath('calibration2.xml')
        data_path = root_path.joinpath('data', self.data_filename)

        self.assertTrue(resistance_calibration1.exists())
        self.assertTrue(resistance_calibration2.exists())
        self.assertTrue(temperature_calibration1.exists())
        self.assertTrue(temperature_calibration2.exists())
        self.assertTrue(data_path.exists())
