#!/usr/bin/env python3
import os

from pyfakefs.fake_filesystem_unittest import TestCase

import date_gap_filler_linker.app as app


class AppTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()

        self.out_path = os.path.join('/', 'pfs', 'out')
        self.in_path = os.path.join('/', 'pfs', 'in')
        self.fs.create_dir(self.out_path)
        self.fs.create_dir(self.in_path)

        self.metadata_1 = os.path.join('prt', '2019', '01', '06', 'location')
        self.metadata_2 = os.path.join('prt', '2019', '01', '07', 'location')
        self.metadata_3 = os.path.join('prt', '2019', '01', '08', 'location')

        # empty files
        data_1 = os.path.join(self.in_path, self.metadata_1, 'data', 'prt_location_2019-01-06.parquet.empty')
        flags_1 = os.path.join(self.in_path, self.metadata_1, 'flags',
                               'prt_location_2019-01-06_flagsCal.parquet.empty')
        location_1 = os.path.join(self.in_path, self.metadata_1, 'location', 'location.json')
        calibration_1 = os.path.join(self.in_path, self.metadata_1, 'calibration')
        uncertainty_1 = os.path.join(self.in_path, self.metadata_1, 'uncertainty_data',
                                     'prt_location_2019-01-06_uncertaintyData.parquet.empty')
        self.fs.create_file(data_1)
        self.fs.create_file(flags_1)
        self.fs.create_file(location_1)
        self.fs.create_dir(calibration_1)
        self.fs.create_file(uncertainty_1)

        # real data
        data_2 = os.path.join(self.in_path, self.metadata_2, 'data', 'prt_location_2019-01-07.parquet')
        data_2_e = os.path.join(self.in_path, self.metadata_2, 'data', 'prt_location_2019-01-07.parquet.empty')
        flags_2 = os.path.join(self.in_path, self.metadata_2, 'flags',
                               'prt_location_2019-01-07_flagsCal.parquet.empty')
        location_2 = os.path.join(self.in_path, self.metadata_2, 'location', 'location.json')
        calibration_2 = os.path.join(self.in_path, self.metadata_2, 'calibration', 'calibration.xml')
        uncertainty_2 = os.path.join(self.in_path, self.metadata_2, 'uncertainty_data',
                                     'prt_location_2019-01-07_uncertaintyData.parquet.empty')
        self.fs.create_file(data_2)
        self.fs.create_file(data_2_e)
        self.fs.create_file(flags_2)
        self.fs.create_file(location_2)
        self.fs.create_file(calibration_2)
        self.fs.create_file(uncertainty_2)

        # real data, no empty file (location not active)
        data_3 = os.path.join(self.in_path, self.metadata_3, 'data', 'prt_location_2019-01-08.parquet')
        flags_3 = os.path.join(self.in_path, self.metadata_3, 'flags', 'prt_location_2019-01-08_flagsCal.parquet')
        location_3 = os.path.join(self.in_path, self.metadata_3, 'location', 'location.json')
        calibration_3 = os.path.join(self.in_path, self.metadata_3, 'calibration', 'calibration.xml')
        uncertainty_3 = os.path.join(self.in_path, self.metadata_3, 'uncertainty_data',
                                     'prt_location_2019-01-08_uncertaintyData.parquet')
        self.fs.create_file(data_3)
        self.fs.create_file(flags_3)
        self.fs.create_file(location_3)
        self.fs.create_file(calibration_3)
        self.fs.create_file(uncertainty_3)

    def test_main(self):
        os.environ['IN_PATH'] = self.in_path
        os.environ['OUT_PATH'] = self.out_path
        os.environ['LOG_LEVEL'] = 'INFO'
        os.environ['RELATIVE_PATH_INDEX'] = '3'
        os.environ['EMPTY_FILE_SUFFIX'] = '.empty'
        app.main()
        self.check_output()

    def check_output(self):
        # empty files
        self.assertTrue(os.path.lexists(os.path.join(self.out_path, self.metadata_1,
                                                     'data', 'prt_location_2019-01-06.parquet')))
        self.assertTrue(os.path.lexists(os.path.join(self.out_path, self.metadata_1, 'flags',
                                                     'prt_location_2019-01-06_flagsCal.parquet')))
        self.assertTrue(os.path.lexists(os.path.join(self.out_path, self.metadata_1, 'location', 'location.json')))
        # empty directory test
        self.assertTrue(os.path.lexists(os.path.join(self.out_path, self.metadata_1, 'calibration')))
        self.assertTrue(os.path.lexists(os.path.join(self.out_path, self.metadata_1, 'uncertainty_data',
                                                     'prt_location_2019-01-06_uncertaintyData.parquet')))

        # real data
        self.assertTrue(os.path.lexists(
            os.path.join(self.out_path, self.metadata_2, 'data', 'prt_location_2019-01-07.parquet')))
        self.assertFalse(os.path.lexists(
            os.path.join(self.out_path, self.metadata_2, 'data', 'prt_location_2019-01-07.parquet.empty')))
        self.assertTrue(os.path.lexists(os.path.join(self.out_path, self.metadata_2, 'flags',
                                                     'prt_location_2019-01-07_flagsCal.parquet')))
        self.assertTrue(os.path.lexists(os.path.join(self.out_path, self.metadata_2, 'location', 'location.json')))
        self.assertTrue(os.path.lexists(os.path.join(self.out_path, self.metadata_2, 'calibration', 'calibration.xml')))
        self.assertTrue(os.path.lexists(os.path.join(self.out_path, self.metadata_2, 'uncertainty_data',
                                                     'prt_location_2019-01-07_uncertaintyData.parquet')))

        # real data, no empty file (location not active)
        self.assertFalse(os.path.lexists(
            os.path.join(self.out_path, self.metadata_3, 'data', 'prt_location_2019-01-08.parquet')))
        self.assertFalse(os.path.lexists(
            os.path.join(self.out_path, self.metadata_3, 'flags', 'prt_location_2019-01-08_flagsCal.parquet')))
        self.assertFalse(os.path.lexists(os.path.join(self.out_path, self.metadata_3, 'location', 'location.json')))
        self.assertFalse(
            os.path.lexists(os.path.join(self.out_path, self.metadata_3, 'calibration', 'calibration.xml')))
        self.assertFalse(os.path.lexists(os.path.join(self.out_path, self.metadata_3, 'uncertainty_data',
                                                      'prt_location_2019-01-08_uncertaintyData.parquet')))
