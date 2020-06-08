#!/usr/bin/env python3
import os
from pathlib import Path

import unittest
from pyfakefs.fake_filesystem_unittest import TestCase

import related_location_group.app as app


class RelatedLocationGroupTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()

        self.input_path = Path('/repo/inputs')
        self.output_path = Path('/outputs')

        self.group = 'aspirated-single-121'

        self.prt_location = 'CFGLOC123'

        self.metadata_path = Path('2019/05/24', self.group)

        self.data_dir = 'data'
        self.location_dir = 'location'

        self.data_file = 'data.extension'
        self.location_file = 'locations.json'

        self.prt_path = Path(self.input_path, 'prt', self.metadata_path)

        self.fs.create_file(Path(self.prt_path, self.prt_location, self.data_dir, self.data_file))
        self.fs.create_file(Path(self.prt_path, self.prt_location, self.location_dir, self.location_file))

        self.source_type_index = 3
        self.year_index = 4
        self.month_index = 5
        self.day_index = 6
        self.group_index = 7
        self.location_index = 8
        self.data_type_index = 9

    def test_group_related(self):
        app.group_related(self.prt_path,
                          self.output_path,
                          self.source_type_index,
                          self.year_index,
                          self.month_index,
                          self.day_index,
                          self.group_index,
                          self.location_index,
                          self.data_type_index)
        self.check_output()

    def test_main(self):
        os.environ['DATA_PATH'] = str(self.prt_path)
        os.environ['OUT_PATH'] = str(self.output_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['SOURCE_TYPE_INDEX'] = str(self.source_type_index)
        os.environ['YEAR_INDEX'] = str(self.year_index)
        os.environ['MONTH_INDEX'] = str(self.month_index)
        os.environ['DAY_INDEX'] = str(self.day_index)
        os.environ['GROUP_INDEX'] = str(self.group_index)
        os.environ['LOCATION_INDEX'] = str(self.location_index)
        os.environ['DATA_TYPE_INDEX'] = str(self.data_type_index)
        app.main()
        self.check_output()

    def check_output(self):
        root_path = Path(self.output_path, self.metadata_path)
        prt_data_path = Path(root_path, 'prt', self.prt_location, self.data_dir, self.data_file)
        prt_location_path = Path(root_path, 'prt', self.prt_location, self.location_dir, self.location_file)
        self.assertTrue(prt_data_path.exists())
        self.assertTrue(prt_location_path.exists())


if __name__ == '__main__':
    unittest.main()
