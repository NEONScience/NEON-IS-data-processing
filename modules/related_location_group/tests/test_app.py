#!/usr/bin/env python3
import os

import unittest
from pyfakefs.fake_filesystem_unittest import TestCase

import related_location_group.app as app


class AppTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()

        self.input_path = os.path.join('/', 'repo', 'inputs')
        self.output_path = os.path.join('/', 'outputs')

        self.group = 'aspirated-single-121'

        self.prt_location = 'CFGLOC123'

        self.metadata_path = os.path.join('2019', '05', '24', self.group)

        self.data_dir = 'data'
        self.location_dir = 'location'

        self.data_file = 'data.extension'
        self.location_file = 'locations.json'

        self.prt_path = os.path.join(self.input_path, 'prt', self.metadata_path)

        self.fs.create_file(os.path.join(self.prt_path, self.prt_location, self.data_dir, self.data_file))
        self.fs.create_file(os.path.join(self.prt_path, self.prt_location, self.location_dir, self.location_file))

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
        os.environ['DATA_PATH'] = self.prt_path
        os.environ['OUT_PATH'] = self.output_path
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
        root_path = os.path.join(self.output_path, self.metadata_path)

        prt_data_path = os.path.join(root_path, 'prt', self.prt_location, self.data_dir, self.data_file)
        prt_location_path = os.path.join(root_path, 'prt', self.prt_location, self.location_dir, self.location_file)
        print(f'prt_data_path: {prt_data_path}')
        print(f'prt_location_path: {prt_location_path}')

        self.assertTrue(os.path.lexists(prt_data_path))
        self.assertTrue(os.path.lexists(prt_location_path))


if __name__ == '__main__':
    unittest.main()
