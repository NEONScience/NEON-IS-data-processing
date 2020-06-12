#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

import data_location_group.data_location_group_main as app
from data_location_group.data_file_path import DataFilePath
from data_location_group.data_location_grouper import DataLocationGrouper


class DataLocationGroupTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = Path('/repo/outputs')
        self.data_path = Path('/repo/data')
        self.location_path = Path('/location')
        self.metadata_path = Path('prt/2019/05/17')
        #  Create data file.
        self.data_file = 'prt_00001_2019-05-17.ext'
        self.input_data_path = self.data_path.joinpath(self.metadata_path, self.data_file)
        self.fs.create_file(self.input_data_path)
        #  Create location file.
        self.location_file = 'prt_00001_locations.json'
        self.input_location_path = self.location_path.joinpath('prt', '00001', self.location_file)
        self.fs.create_file(self.input_location_path)
        #  Create output directory.
        self.fs.create_dir(self.out_path)
        #  File path indices.
        self.source_type_index = 3
        self.year_index = 4
        self.month_index = 5
        self.day_index = 6
        self.file_index = 7

    def test_group(self):
        data_file_path = DataFilePath(source_type_index=self.source_type_index,
                                      year_index=self.year_index,
                                      month_index=self.month_index,
                                      day_index=self.day_index)
        data_location_grouper = DataLocationGrouper(data_path=self.data_path, location_path=self.location_path,
                                                    out_path=self.out_path, data_file_path=data_file_path)
        data_location_grouper.group_files()
        self.check_output()

    def test_main(self):
        os.environ['DATA_PATH'] = str(self.data_path)
        os.environ['LOCATION_PATH'] = str(self.location_path)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['SOURCE_TYPE_INDEX'] = str(self.source_type_index)
        os.environ['YEAR_INDEX'] = str(self.year_index)
        os.environ['MONTH_INDEX'] = str(self.month_index)
        os.environ['DAY_INDEX'] = str(self.day_index)
        os.environ['FILE_INDEX'] = str(self.file_index)
        app.main()
        self.check_output()

    def check_output(self):
        root_path = Path(self.out_path, self.metadata_path, '00001')
        output_data_path = root_path.joinpath('data', self.data_file)
        output_location_path = root_path.joinpath('location', self.location_file)
        self.assertTrue(output_data_path.exists())
        self.assertTrue(output_location_path.exists())
