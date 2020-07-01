#!/usr/bin/env python3
import os
from pathlib import Path

import unittest
from pyfakefs.fake_filesystem_unittest import TestCase

from related_location_group.related_location_group_config import Config
from related_location_group.related_location_grouper import RelatedLocationGrouper
import related_location_group.related_location_group_main as related_location_group_main


class RelatedLocationGroupTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.source_type = 'prt'
        self.in_path = Path('/in/repo')
        self.out_path = Path('/out')
        self.group = 'aspirated-single-121'
        self.location = 'CFGLOC123'
        self.metadata_path = Path('2019/05/24', self.group)
        self.data_dir = 'data'
        self.location_dir = 'location'
        self.data_file = 'data.extension'
        self.location_file = 'locations.json'
        self.prt_path = Path(self.in_path, self.source_type, self.metadata_path)
        self.fs.create_file(Path(self.prt_path, self.location, self.data_dir, self.data_file))
        self.fs.create_file(Path(self.prt_path, self.location, self.location_dir, self.location_file))
        # path indices
        self.source_type_index = 3
        self.year_index = 4
        self.month_index = 5
        self.day_index = 6
        self.group_index = 7
        self.location_index = 8
        self.data_type_index = 9

    def test_group_files(self):
        config = Config(data_path=self.in_path,
                        out_path=self.out_path,
                        source_type_index=self.source_type_index,
                        year_index=self.year_index,
                        month_index=self.month_index,
                        day_index=self.day_index,
                        group_index=self.group_index,
                        location_index=self.location_index,
                        data_type_index=self.data_type_index)
        related_location_grouper = RelatedLocationGrouper(config)
        related_location_grouper.group_files()
        self.check_output()

    def test_main(self):
        os.environ['DATA_PATH'] = str(self.in_path)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['SOURCE_TYPE_INDEX'] = str(self.source_type_index)
        os.environ['YEAR_INDEX'] = str(self.year_index)
        os.environ['MONTH_INDEX'] = str(self.month_index)
        os.environ['DAY_INDEX'] = str(self.day_index)
        os.environ['GROUP_INDEX'] = str(self.group_index)
        os.environ['LOCATION_INDEX'] = str(self.location_index)
        os.environ['DATA_TYPE_INDEX'] = str(self.data_type_index)
        related_location_group_main.main()
        self.check_output()

    def check_output(self):
        root_path = Path(self.out_path, self.metadata_path)
        data_path = Path(root_path, self.source_type, self.location, self.data_dir, self.data_file)
        location_path = Path(root_path, self.source_type, self.location, self.location_dir, self.location_file)
        self.assertTrue(data_path.exists())
        self.assertTrue(location_path.exists())


if __name__ == '__main__':
    unittest.main()
