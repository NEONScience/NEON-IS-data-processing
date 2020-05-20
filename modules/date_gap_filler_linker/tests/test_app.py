#!/usr/bin/env python3
import os

from pyfakefs.fake_filesystem_unittest import TestCase

import date_gap_filler_linker.app as app


class AppTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()

        self.out_path = os.path.join('/', 'pfs', 'out')
        self.in_path = os.path.join('/', 'pfs', 'repo')
        self.fs.create_dir(self.out_path)
        self.fs.create_dir(self.in_path)

        self.metadata_1 = os.path.join('prt', '2019', '01', '06', 'CFGLOC113499')
        self.metadata_2 = os.path.join('prt', '2019', '01', '07', 'CFGLOC113499')
        self.metadata_3 = os.path.join('prt', '2019', '01', '08', 'CFGLOC113499')

        # empty files
        data_1 = os.path.join(self.in_path, self.metadata_1, 'data', 'prt_CFGLOC113499_2019-01-06.parquet.empty')
        flags_1 = os.path.join(self.in_path, self.metadata_1, 'flags',
                               'prt_CFGLOC113499_2019-01-06_flagsCal.parquet.empty')
        location_1 = os.path.join(self.in_path, self.metadata_1, 'location', 'CFGLOC113499.json')
        calibration_1 = os.path.join(self.in_path, self.metadata_1, 'calibration', 'calibration.xml')
        uncertainty_1 = os.path.join(self.in_path, self.metadata_1, 'uncertainty_data',
                                     'prt_CFGLOC113499_2019-01-06_uncertaintyData.parquet.empty')
        self.fs.create_file(data_1)
        self.fs.create_file(flags_1)
        self.fs.create_file(location_1)
        self.fs.create_file(calibration_1)
        self.fs.create_file(uncertainty_1)

        # real data
        data_2 = os.path.join(self.in_path, self.metadata_2, 'data', 'prt_CFGLOC113499_2019-01-07.parquet')
        data_2_e = os.path.join(self.in_path, self.metadata_2, 'data', 'prt_CFGLOC113499_2019-01-07.parquet.empty')
        flags_2 = os.path.join(self.in_path, self.metadata_2, 'flags',
                               'prt_CFGLOC113499_2019-01-07_flagsCal.parquet.empty')
        location_2 = os.path.join(self.in_path, self.metadata_2, 'location', 'CFGLOC113499.json')
        calibration_2 = os.path.join(self.in_path, self.metadata_2, 'calibration', 'calibration.xml')
        uncertainty_2 = os.path.join(self.in_path, self.metadata_2, 'uncertainty_data',
                                     'prt_CFGLOC113499_2019-01-07_uncertaintyData.parquet.empty')
        self.fs.create_file(data_2)
        self.fs.create_file(data_2_e)
        self.fs.create_file(flags_2)
        self.fs.create_file(location_2)
        self.fs.create_file(calibration_2)
        self.fs.create_file(uncertainty_2)

        # real data, no empty file (location not active)
        data_3 = os.path.join(self.in_path, self.metadata_3, 'data', 'prt_CFGLOC113499_2019-01-08.parquet')
        flags_3 = os.path.join(self.in_path, self.metadata_3, 'flags', 'prt_CFGLOC113499_2019-01-08_flagsCal.parquet')
        location_3 = os.path.join(self.in_path, self.metadata_3, 'location', 'CFGLOC113499.json')
        calibration_3 = os.path.join(self.in_path, self.metadata_3, 'calibration', 'calibration.xml')
        uncertainty_3 = os.path.join(self.in_path, self.metadata_3, 'uncertainty_data',
                                     'prt_CFGLOC113499_2019-01-08_uncertaintyData.parquet')
        self.fs.create_file(data_3)
        self.fs.create_file(flags_3)
        self.fs.create_file(location_3)
        self.fs.create_file(calibration_3)
        self.fs.create_file(uncertainty_3)

        # path indices
        self.source_type_index = '3'
        self.year_index = '4'
        self.month_index = '5'
        self.day_index = '6'
        self.location_index = '7'
        self.data_type_index = '8'
        self.filename_index = '9'

    def test_main(self):
        os.environ['IN_PATH'] = self.in_path
        os.environ['OUT_PATH'] = self.out_path
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['SOURCE_TYPE_INDEX'] = self.source_type_index
        os.environ['YEAR_INDEX'] = self.year_index
        os.environ['MONTH_INDEX'] = self.month_index
        os.environ['DAY_INDEX'] = self.day_index
        os.environ['LOCATION_INDEX'] = self.location_index
        os.environ['DATA_TYPE_INDEX'] = self.data_type_index
        os.environ['FILENAME_INDEX'] = self.filename_index
        app.main()
        self.check_output()

    def check_output(self):
        # empty files
        self.assertTrue(os.path.lexists(os.path.join(self.out_path, self.metadata_1,
                                                     'data', 'prt_CFGLOC113499_2019-01-06.parquet')))
        self.assertTrue(os.path.lexists(os.path.join(self.out_path, self.metadata_1, 'flags',
                                                     'prt_CFGLOC113499_2019-01-06_flagsCal.parquet')))
        self.assertTrue(os.path.lexists(os.path.join(self.out_path, self.metadata_1, 'location', 'CFGLOC113499.json')))
        self.assertTrue(os.path.lexists(os.path.join(self.out_path, self.metadata_1, 'calibration', 'calibration.xml')))
        self.assertTrue(os.path.lexists(os.path.join(self.out_path, self.metadata_1, 'uncertainty_data',
                                                     'prt_CFGLOC113499_2019-01-06_uncertaintyData.parquet')))

        # real data
        self.assertTrue(os.path.lexists(
            os.path.join(self.out_path, self.metadata_2, 'data', 'prt_CFGLOC113499_2019-01-07.parquet')))
        self.assertFalse(os.path.lexists(
            os.path.join(self.out_path, self.metadata_2, 'data', 'prt_CFGLOC113499_2019-01-07.parquet.empty')))
        self.assertTrue(os.path.lexists(os.path.join(self.out_path, self.metadata_2, 'flags',
                                                     'prt_CFGLOC113499_2019-01-07_flagsCal.parquet')))
        self.assertTrue(os.path.lexists(os.path.join(self.out_path, self.metadata_2, 'location', 'CFGLOC113499.json')))
        self.assertTrue(os.path.lexists(os.path.join(self.out_path, self.metadata_2, 'calibration', 'calibration.xml')))
        self.assertTrue(os.path.lexists(os.path.join(self.out_path, self.metadata_2, 'uncertainty_data',
                                                     'prt_CFGLOC113499_2019-01-07_uncertaintyData.parquet')))

        # real data, no empty file (location not active)
        self.assertFalse(os.path.lexists(
            os.path.join(self.out_path, self.metadata_3, 'data', 'prt_CFGLOC113499_2019-01-08.parquet')))
        self.assertFalse(os.path.lexists(
            os.path.join(self.out_path, self.metadata_3, 'flags', 'prt_CFGLOC113499_2019-01-08_flagsCal.parquet')))
        self.assertFalse(os.path.lexists(os.path.join(self.out_path, self.metadata_3, 'location', 'CFGLOC113499.json')))
        self.assertFalse(
            os.path.lexists(os.path.join(self.out_path, self.metadata_3, 'calibration', 'calibration.xml')))
        self.assertFalse(os.path.lexists(os.path.join(self.out_path, self.metadata_3, 'uncertainty_data',
                                                      'prt_CFGLOC113499_2019-01-08_uncertaintyData.parquet')))
