#!/usr/bin/env python3
import os
import unittest
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

from calibrated_location_group.calibrated_location_group_config import Config
from calibrated_location_group.calibrated_location_file_grouper import CalibratedLocationFileGrouper
import calibrated_location_group.calibrated_location_group_main as calibrated_location_group_main


class CalibratedLocationGroupTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()

        self.out_path = Path('/out')
        self.calibrated_path = Path('/in/calibrated')
        self.location_path = Path('/in/location')
        self.metadata_path_1 = Path('prt/2019/01/01/00001')
        self.metadata_path_2 = Path('prt/2019/01/02/00001')

        #  Create day 1 calibrated input files.
        calibrated_root_1 = Path(self.calibrated_path, self.metadata_path_1)
        data_path_1 = Path(calibrated_root_1, 'data/data.ext')
        flags_path_1 = Path(calibrated_root_1, 'flags/flags.ext')
        uncertainty_path_1 = Path(calibrated_root_1, 'uncertainty/uncertainty.json')
        test_extra_dir_path_1 = Path(calibrated_root_1, 'test/test_dir/test.json')

        self.fs.create_file(data_path_1)
        self.fs.create_file(flags_path_1)
        self.fs.create_file(uncertainty_path_1)
        self.fs.create_file(test_extra_dir_path_1)

        #  Create day 2 calibrated input files.
        calibrated_root_2 = Path(self.calibrated_path, self.metadata_path_2)
        data_path_2 = Path(calibrated_root_2, 'data/data.ext')
        flags_path_2 = Path(calibrated_root_2, 'flags/flags.ext')
        uncertainty_path_2 = Path(calibrated_root_2, 'uncertainty/uncertainty.json')
        # test_extra_dir_path_2 = Path(calibrated_root_2, 'test/test_dir/test.json')

        self.fs.create_file(data_path_2)
        self.fs.create_file(flags_path_2)
        self.fs.create_file(uncertainty_path_2)
        # self.fs.create_file(test_extra_dir_path_2)

        #  Create location input file.
        locations_path = Path(self.location_path, 'prt/00001/locations.json')
        self.fs.create_file(locations_path)

        self.source_type_index = 3
        self.year_index = 4
        self.month_index = 5
        self.day_index = 6
        self.source_id_index = 7
        self.data_type_index = 8

    def test_group(self):
        config = Config(calibrated_path=self.calibrated_path,
                        location_path=self.location_path,
                        out_path=self.out_path,
                        source_type_index=self.source_type_index,
                        year_index=self.year_index,
                        month_index=self.month_index,
                        day_index=self.day_index,
                        source_id_index=self.source_id_index,
                        data_type_index=self.data_type_index)
        grouper = CalibratedLocationFileGrouper(config)
        grouper.group_files()

    @unittest.skip('')
    def test_main(self):
        os.environ['CALIBRATED_PATH'] = str(self.calibrated_path)
        os.environ['LOCATION_PATH'] = str(self.location_path)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['SOURCE_TYPE_INDEX'] = str(self.source_type_index)
        os.environ['YEAR_INDEX'] = str(self.year_index)
        os.environ['MONTH_INDEX'] = str(self.month_index)
        os.environ['DAY_INDEX'] = str(self.day_index)
        os.environ['SOURCE_ID_INDEX'] = str(self.source_id_index)
        os.environ['DATA_TYPE_INDEX'] = str(self.data_type_index)
        calibrated_location_group_main.main()
        self.check_output()

    def check_output(self):
        # check day 1
        root_path_1 = Path(self.out_path, self.metadata_path_1)
        data_path_1 = Path(root_path_1, 'data/data.ext')
        flags_path_1 = Path(root_path_1, 'flags/flags.ext')
        locations_path_1 = Path(root_path_1, 'location/locations.json')
        uncertainty_path_1 = Path(root_path_1, 'uncertainty/uncertainty.json')
        test_extra_dir_path_1 = Path(root_path_1, 'test/test_dir/test.json')
        self.assertTrue(data_path_1.exists())
        self.assertTrue(flags_path_1.exists())
        self.assertTrue(locations_path_1.exists())
        self.assertTrue(uncertainty_path_1.exists())
        self.assertTrue(test_extra_dir_path_1.exists())

        # check day 2
        root_path_2 = Path(self.out_path, self.metadata_path_2)
        data_path_2 = Path(root_path_2, 'data/data.ext')
        flags_path_2 = Path(root_path_2, 'flags/flags.ext')
        locations_path_2 = Path(root_path_2, 'location/locations.json')
        uncertainty_path_2 = Path(root_path_2, 'uncertainty/uncertainty.json')
        # test_extra_dir_path_2 = Path(root_path_2, 'test/test_dir/test.json')
        self.assertTrue(data_path_2.exists())
        self.assertTrue(flags_path_2.exists())
        self.assertTrue(locations_path_2.exists())
        self.assertTrue(uncertainty_path_2.exists())
        # self.assertTrue(test_extra_dir_path_2.exists())
