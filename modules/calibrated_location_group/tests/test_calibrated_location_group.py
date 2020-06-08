#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

import calibrated_location_group.app as app
from common import log_config as log_config


class CalibratedLocationGroupTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()

        self.out_path = Path('/outputs')
        self.metadata_path = Path('prt/2019/05/17/00001')
        self.calibrated_path = Path('/inputs/calibrated')
        self.location_path = Path('/inputs/location')

        #  Create calibrated input files.
        calibrated_root = self.calibrated_path.joinpath(self.metadata_path)
        data_path = calibrated_root.joinpath('data', 'data.ext')
        flags_path = calibrated_root.joinpath('flags', 'flags.ext')
        uncertainty_path = calibrated_root.joinpath('uncertainty', 'uncertainty.json')
        test_extra_dir_path = calibrated_root.joinpath('test', 'test_dir', 'test.json')

        self.fs.create_file(data_path)
        self.fs.create_file(flags_path)
        self.fs.create_file(uncertainty_path)
        self.fs.create_file(test_extra_dir_path)

        #  Create location input file.
        locations_path = Path(self.location_path, 'prt/00001/locations.json')
        self.fs.create_file(locations_path)

        #  Create output dir
        self.fs.create_dir(self.out_path)

        self.source_type_index = '3'
        self.year_index = '4'
        self.month_index = '5'
        self.day_index = '6'
        self.source_id_index = '7'
        self.data_type_index = '8'

    def test_main(self):
        os.environ['CALIBRATED_PATH'] = str(self.calibrated_path)
        os.environ['LOCATION_PATH'] = str(self.location_path)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['SOURCE_TYPE_INDEX'] = self.source_type_index
        os.environ['YEAR_INDEX'] = self.year_index
        os.environ['MONTH_INDEX'] = self.month_index
        os.environ['DAY_INDEX'] = self.day_index
        os.environ['SOURCE_ID_INDEX'] = self.source_id_index
        os.environ['DATA_TYPE_INDEX'] = self.data_type_index
        app.main()
        self.check_output()

    def check_output(self):
        root_path = Path(self.out_path, self.metadata_path)
        data_path = root_path.joinpath('data', 'data.ext')
        flags_path = root_path.joinpath('flags', 'flags.ext')
        locations_path = root_path.joinpath('location', 'locations.json')
        uncertainty_path = root_path.joinpath('uncertainty', 'uncertainty.json')
        test_extra_dir_path = root_path.joinpath('test', 'test_dir', 'test.json')
        self.assertTrue(data_path.exists())
        self.assertTrue(flags_path.exists())
        self.assertTrue(locations_path.exists())
        self.assertTrue(uncertainty_path.exists())
        self.assertTrue(test_extra_dir_path.exists())
