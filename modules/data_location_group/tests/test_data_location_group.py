#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

import data_location_group.data_location_group_main as data_location_group_main
from data_location_group.data_location_group_config import Config
from data_location_group.data_location_grouper import DataLocationGrouper


class DataLocationGroupTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = Path('/out')
        self.data_path = Path('/in/data')
        self.location_path = Path('/in/location')
        self.metadata_path = Path('prt/2019/05/17/00001')
        #  Create data file.
        self.data_file = 'prt_00001_2019-05-17.ext'
        data_file_path = Path(self.data_path, self.metadata_path, self.data_file)
        self.fs.create_file(data_file_path)
        #  Create location file.
        self.location_file = 'prt_00001_locations.json'
        location_file_path = Path(self.location_path, 'prt', '00001', self.location_file)
        self.fs.create_file(location_file_path)
        #  Create output directory.
        self.fs.create_dir(self.out_path)
        #  File path indices.
        self.source_type_index = 3
        self.year_index = 4
        self.month_index = 5
        self.day_index = 6
        self.source_id_index = 7

    def test_group(self):
        config = Config(data_path=self.data_path,
                        location_path=self.location_path,
                        out_path=self.out_path,
                        source_type_index=self.source_type_index,
                        year_index=self.year_index,
                        month_index=self.month_index,
                        day_index=self.day_index,
                        source_id_index=self.source_id_index)
        data_location_grouper = DataLocationGrouper(config)
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
        os.environ['SOURCE_ID_INDEX'] = str(self.source_id_index)
        data_location_group_main.main()
        self.check_output()

    def check_output(self):
        root_path = Path(self.out_path, self.metadata_path)
        data_path = Path(root_path, 'data', self.data_file)
        location_path = Path(root_path, 'location', self.location_file)
        self.assertTrue(data_path.exists())
        self.assertTrue(location_path.exists())
