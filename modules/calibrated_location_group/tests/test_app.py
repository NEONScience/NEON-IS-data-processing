#!/usr/bin/env python3
import os

from pyfakefs.fake_filesystem_unittest import TestCase

import calibrated_location_group.app as app
from lib import log_config as log_config


class AppTest(TestCase):

    def setUp(self):

        log_config.configure('DEBUG')

        self.setUpPyfakefs()

        self.out_path = os.path.join('/', 'outputs')
        self.metadata_path = os.path.join('prt', '2019', '05', '17', '00001')
        self.calibrated_path = os.path.join('/', 'inputs', 'calibrated')
        self.location_path = os.path.join('/', 'inputs', 'location')

        #  Create calibrated input files.
        calibrated_root = os.path.join(self.calibrated_path, self.metadata_path)
        data_path = os.path.join(calibrated_root, 'data', 'data.ext')
        flags_path = os.path.join(calibrated_root, 'flags', 'flags.ext')
        uncertainty_path = os.path.join(calibrated_root, 'uncertainty', 'uncertainty.json')
        test_extra_dir_path = os.path.join(calibrated_root, 'test', 'test_dir', 'test.json')

        self.fs.create_file(data_path)
        self.fs.create_file(flags_path)
        self.fs.create_file(uncertainty_path)
        self.fs.create_file(test_extra_dir_path)

        #  Create location input file.
        locations_path = os.path.join(self.location_path, 'prt', '00001', 'locations.json')
        self.fs.create_file(locations_path)

        #  Create output dir
        self.fs.create_dir(self.out_path)

        self.source_type_index = '3'
        self.year_index = '4'
        self.month_index = '5'
        self.day_index = '6'
        self.source_id_index = '7'
        self.data_type_index = '8'

    def test_group(self):
        app.group(self.calibrated_path,
                  self.location_path,
                  self.out_path,
                  int(self.source_type_index),
                  int(self.year_index),
                  int(self.month_index),
                  int(self.day_index),
                  int(self.source_id_index),
                  int(self.data_type_index))
        self.check_output()

    def test_main(self):
        os.environ['CALIBRATED_PATH'] = self.calibrated_path
        os.environ['LOCATION_PATH'] = self.location_path
        os.environ['SOURCE_TYPE_INDEX'] = self.source_type_index
        os.environ['YEAR_INDEX'] = self.year_index
        os.environ['MONTH_INDEX'] = self.month_index
        os.environ['DAY_INDEX'] = self.day_index
        os.environ['SOURCE_ID_INDEX'] = self.source_id_index
        os.environ['DATA_TYPE_INDEX'] = self.data_type_index
        os.environ['OUT_PATH'] = self.out_path
        os.environ['LOG_LEVEL'] = 'DEBUG'
        app.main()
        self.check_output()

    def check_output(self):
        root_path = os.path.join(self.out_path, self.metadata_path)
        data_path = os.path.join(root_path, 'data', 'data.ext')
        flags_path = os.path.join(root_path, 'flags', 'flags.ext')
        locations_path = os.path.join(root_path, 'location', 'locations.json')
        uncertainty_path = os.path.join(root_path, 'uncertainty', 'uncertainty.json')
        test_extra_dir_path = os.path.join(root_path, 'test', 'test_dir', 'test.json')
        self.assertTrue(os.path.lexists(data_path))
        self.assertTrue(os.path.lexists(flags_path))
        self.assertTrue(os.path.lexists(locations_path))
        self.assertTrue(os.path.lexists(uncertainty_path))
        self.assertTrue(os.path.lexists(test_extra_dir_path))
