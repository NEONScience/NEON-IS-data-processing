#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

import date_gap_filler_linker.app as app


class DateGapFillerTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()

        self.out_path = Path('/pfs/out')
        self.in_path = Path('/pfs/in')
        self.fs.create_dir(self.out_path)
        self.fs.create_dir(self.in_path)

        self.metadata_1 = Path('prt/2019/01/01/CFG123')
        self.metadata_2 = Path('prt/2019/01/02/CFG123')
        self.metadata_3 = Path('prt/2019/01/03/CFG123')

        # empty files
        data_1 = self.in_path.joinpath(self.metadata_1, 'data/prt_CFG123_2019-01-01.parquet.empty')
        flags_1 = self.in_path.joinpath(self.metadata_1, 'flags/prt_CFG123_2019-01-01_flagsCal.parquet.empty')
        location_1 = self.in_path.joinpath(self.metadata_1, 'location/location.json')
        calibration_1 = self.in_path.joinpath(self.metadata_1, 'calibration')
        uncertainty_1 = self.in_path.joinpath(self.metadata_1, 'uncertainty_data',
                                              'prt_CFG123_2019-01-01_uncertaintyData.parquet.empty')
        self.fs.create_file(data_1)
        self.fs.create_file(flags_1)
        self.fs.create_file(location_1)
        self.fs.create_dir(calibration_1)
        self.fs.create_file(uncertainty_1)

        # real data
        data_2 = self.in_path.joinpath(self.metadata_2, 'data/prt_CFG123_2019-01-02.parquet')
        data_2_e = self.in_path.joinpath(self.metadata_2, 'data/prt_CFG123_2019-01-02.parquet.empty')
        flags_2 = self.in_path.joinpath(self.metadata_2, 'flags/prt_CFG123_2019-01-02_flagsCal.parquet.empty')
        location_2 = self.in_path.joinpath(self.metadata_2, 'location/location.json')
        calibration_2 = self.in_path.joinpath(self.metadata_2, 'calibration/calibration.xml')
        uncertainty_2 = self.in_path.joinpath(self.metadata_2, 'uncertainty_data',
                                              'prt_CFG123_2019-01-02_uncertaintyData.parquet.empty')
        self.fs.create_file(data_2)
        self.fs.create_file(data_2_e)
        self.fs.create_file(flags_2)
        self.fs.create_file(location_2)
        self.fs.create_file(calibration_2)
        self.fs.create_file(uncertainty_2)

        # real data, no empty file (location not active)
        data_3 = self.in_path.joinpath(self.metadata_3, 'data/prt_CFG123_2019-01-02.parquet')
        flags_3 = self.in_path.joinpath(self.metadata_3, 'flags/prt_CFG123_2019-01-02_flagsCal.parquet')
        location_3 = self.in_path.joinpath(self.metadata_3, 'location/location.json')
        calibration_3 = self.in_path.joinpath(self.metadata_3, 'calibration/calibration.xml')
        uncertainty_3 = self.in_path.joinpath(self.metadata_3, 'uncertainty_data',
                                              'prt_CFG123_2019-01-02_uncertaintyData.parquet')
        self.fs.create_file(data_3)
        self.fs.create_file(flags_3)
        self.fs.create_file(location_3)
        self.fs.create_file(calibration_3)
        self.fs.create_file(uncertainty_3)

    def test_main(self):
        os.environ['IN_PATH'] = str(self.in_path)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['RELATIVE_PATH_INDEX'] = '3'
        os.environ['LOCATION_INDEX'] = '7'
        os.environ['EMPTY_FILE_SUFFIX'] = '.empty'
        app.main()
        self.check_output()

    def check_output(self):
        # empty files
        self.assertTrue(Path(self.out_path, self.metadata_1, 'data/prt_CFG123_2019-01-01.parquet').exists())
        self.assertTrue(Path(self.out_path, self.metadata_1, 'flags/prt_CFG123_2019-01-01_flagsCal.parquet').exists())
        self.assertTrue(Path(self.out_path, self.metadata_1, 'location/location.json').exists())
        # empty directory test
        self.assertTrue(Path(self.out_path, self.metadata_1, 'calibration').exists())
        self.assertTrue(Path(self.out_path, self.metadata_1, 'uncertainty_data',
                             'prt_CFG123_2019-01-01_uncertaintyData.parquet').exists())

        # real data
        self.assertTrue(Path(self.out_path, self.metadata_2, 'data/prt_CFG123_2019-01-02.parquet').exists())
        self.assertFalse(Path(self.out_path, self.metadata_2, 'data/prt_CFG123_2019-01-02.parquet.empty').exists())
        self.assertTrue(Path(self.out_path, self.metadata_2, 'flags/prt_CFG123_2019-01-02_flagsCal.parquet').exists())
        self.assertTrue(Path(self.out_path, self.metadata_2, 'location/location.json').exists())
        self.assertTrue(Path(self.out_path, self.metadata_2, 'calibration/calibration.xml').exists())
        self.assertTrue(Path(self.out_path, self.metadata_2, 'uncertainty_data',
                             'prt_CFG123_2019-01-02_uncertaintyData.parquet').exists())

        # real data, but no empty files (the location is not active)
        self.assertFalse(Path(self.out_path, self.metadata_3).exists())
        self.assertFalse(Path(self.out_path, self.metadata_3, 'data/prt_CFG123_2019-01-03.parquet').exists())
        self.assertFalse(Path(self.out_path, self.metadata_3, 'flags/prt_CFG123_2019-01-03_flagsCal.parquet').exists())
        self.assertFalse(Path(self.out_path, self.metadata_3, 'location/location.json').exists())
        self.assertFalse(Path(self.out_path, self.metadata_3, 'calibration/calibration.xml').exists())
        self.assertFalse(Path(self.out_path, self.metadata_3, 'uncertainty_data',
                              'prt_CFG123_2019-01-03_uncertaintyData.parquet').exists())
