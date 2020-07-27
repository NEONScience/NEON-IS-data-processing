#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

from data_calibration_group.data_calibration_group_config import Config
from data_calibration_group.data_calibration_grouper import group_files
import data_calibration_group.data_calibration_group_main as data_calibration_group_main


class DataCalibrationGroupNoCalibrationTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()

        in_path = Path('/in')
        self.out_path = Path('/out')
        self.calibration_path = Path(in_path, 'calibration')
        self.data_path = Path(in_path, 'data')
        self.data_metadata_path = Path('prt/0001/2019/07/23')
        self.calibration_metadata_path = Path('prt/0001')
        self.data_filename = 'prt_0001_2019-07-23.ext'

        self.fs.create_file(Path(in_path, 'data', self.data_metadata_path, self.data_filename))
        self.fs.create_dir(Path(self.calibration_path, self.calibration_metadata_path))

        self.data_source_type_index = 3
        self.data_source_id_index = 4
        self.data_year_index = 5
        self.data_month_index = 6
        self.data_day_index = 7
        self.calibration_source_type_index = 3
        self.calibration_source_id_index = 4
        self.calibration_stream_index = 5

    def test_group_files(self):
        config = Config(data_path=self.data_path,
                        calibration_path=self.calibration_path,
                        out_path=self.out_path,
                        data_source_type_index=self.data_source_type_index,
                        data_source_id_index=self.data_source_id_index,
                        data_year_index=self.data_year_index,
                        data_month_index=self.data_month_index,
                        data_day_index=self.data_day_index,
                        calibration_source_type_index=self.calibration_source_type_index,
                        calibration_source_id_index=self.calibration_source_id_index,
                        calibration_stream_index=self.calibration_stream_index)
        group_files(config)
        self.check_output()

    def test_app(self):
        os.environ['DATA_PATH'] = str(self.data_path)
        os.environ['CALIBRATION_PATH'] = str(self.calibration_path)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['DATA_SOURCE_TYPE_INDEX'] = str(self.data_source_type_index)
        os.environ['DATA_SOURCE_ID_INDEX'] = str(self.data_source_id_index)
        os.environ['DATA_YEAR_INDEX'] = str(self.data_year_index)
        os.environ['DATA_MONTH_INDEX'] = str(self.data_month_index)
        os.environ['DATA_DAY_INDEX'] = str(self.data_day_index)
        os.environ['CALIBRATION_SOURCE_TYPE_INDEX'] = str(self.calibration_source_type_index)
        os.environ['CALIBRATION_SOURCE_ID_INDEX'] = str(self.calibration_source_id_index)
        os.environ['CALIBRATION_STREAM_INDEX'] = str(self.calibration_stream_index)
        data_calibration_group_main.main()
        self.check_output()

    def check_output(self):
        root_path = Path(self.out_path, 'prt/2019/07/23/0001')
        calibration_path = Path(root_path, 'calibration')
        data_path = Path(root_path, 'data', self.data_filename)
        metadata_path = Path(calibration_path, self.calibration_metadata_path)
        self.assertTrue(calibration_path.exists())
        self.assertTrue(data_path.exists())
        self.assertFalse(metadata_path.exists())
