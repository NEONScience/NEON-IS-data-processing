#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

from calibrated_location_group.calibrated_file_path import CalibratedFilePath
from calibrated_location_group.calibrated_location_file_grouper import CalibratedLocationFileGrouper
import calibrated_location_group.calibrated_location_group_main as calibrated_location_group_main


class CalibratedLocationGroupTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()

        self.out_path = Path('/out')
        self.calibrated_path = Path('/in/calibrated')
        self.location_path = Path('/in/location')
        self.metadata_path = Path('prt/2019/05/17/00001')

        #  Create calibrated input files.
        calibrated_root = Path(self.calibrated_path, self.metadata_path)
        data_path = Path(calibrated_root, 'data/data.ext')
        flags_path = Path(calibrated_root, 'flags/flags.ext')
        uncertainty_path = Path(calibrated_root, 'uncertainty/uncertainty.json')
        test_extra_dir_path = Path(calibrated_root, 'test/test_dir/test.json')

        self.fs.create_file(data_path)
        self.fs.create_file(flags_path)
        self.fs.create_file(uncertainty_path)
        self.fs.create_file(test_extra_dir_path)

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
        calibrated_file_path = CalibratedFilePath(source_type_index=self.source_type_index,
                                                  year_index=self.year_index,
                                                  month_index=self.month_index,
                                                  day_index=self.day_index,
                                                  source_id_index=self.source_id_index,
                                                  data_type_index=self.data_type_index)
        grouper = CalibratedLocationFileGrouper(calibrated_path=self.calibrated_path,
                                                location_path=self.location_path,
                                                out_path=self.out_path,
                                                calibrated_file_path=calibrated_file_path)
        grouper.group_files()

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
        root_path = Path(self.out_path, self.metadata_path)
        data_path = Path(root_path, 'data/data.ext')
        flags_path = Path(root_path, 'flags/flags.ext')
        locations_path = Path(root_path, 'location/locations.json')
        uncertainty_path = Path(root_path, 'uncertainty/uncertainty.json')
        test_extra_dir_path = Path(root_path, 'test/test_dir/test.json')
        self.assertTrue(data_path.exists())
        self.assertTrue(flags_path.exists())
        self.assertTrue(locations_path.exists())
        self.assertTrue(uncertainty_path.exists())
        self.assertTrue(test_extra_dir_path.exists())
