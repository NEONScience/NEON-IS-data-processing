import os

from pyfakefs.fake_filesystem_unittest import TestCase

import data_calibration_group.app as app
import data_calibration_group.grouper as grouper
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

    def test_grouper(self):
        grouper.group(self.data_path, self.calibration_path, self.out_path)
        self.check_output()

    def test_main(self):
        os.environ['DATA_PATH'] = self.data_path
        os.environ['CALIBRATION_PATH'] = self.calibration_path
        os.environ['OUT_PATH'] = self.out_path
        os.environ['LOG_LEVEL'] = 'DEBUG'
        app.main()
        self.check_output()

    def check_output(self):
        output_root = os.path.join(self.out_path, self.data_metadata_path)
        calibration_path = os.path.join(output_root, 'calibration')
        data_path = os.path.join(output_root, 'data', self.data_filename)
        self.assertTrue(os.path.lexists(calibration_path))
        self.assertTrue(os.path.lexists(data_path))
        self.assertFalse(os.path.lexists(os.path.join(calibration_path, self.calibration_metadata_path)))
