#!/usr/bin/env python3
import os

from pyfakefs.fake_filesystem_unittest import TestCase

import data_location_group.app as app
from data_location_group.data_location_grouper import link_data, link_location
from lib import log_config as log_config


class AppTest(TestCase):

    def setUp(self):
        log_config.configure('DEBUG')

        self.setUpPyfakefs()

        self.out_path = os.path.join('/', 'repo', 'outputs')
        self.data_path = os.path.join('/', 'repo', 'data')
        self.location_path = os.path.join('/', 'location')
        self.metadata_path = os.path.join('prt', '2019', '05', '17')

        #  Create data file.
        self.data_file = 'prt_00001_2019-05-17.ext'
        self.input_data_path = os.path.join(self.data_path, self.metadata_path, self.data_file)
        self.fs.create_file(self.input_data_path)

        #  Create location file.
        self.location_file = 'prt_00001_locations.json'
        self.input_location_path = os.path.join(self.location_path, 'prt', '00001', self.location_file)
        self.fs.create_file(self.input_location_path)

        #  Create output directory.
        self.fs.create_dir(self.out_path)

        #  File path indices.
        self.source_type_index = '3'
        self.year_index = '4'
        self.month_index = '5'
        self.day_index = '6'
        self.file_index = '7'

    def test_group(self):
        for output_dir in link_data(self.data_path,
                                    self.out_path,
                                    int(self.source_type_index),
                                    int(self.year_index),
                                    int(self.month_index),
                                    int(self.day_index),
                                    int(self.file_index)):
            link_location(self.location_path, output_dir)
        self.check_output()

    def test_main(self):
        os.environ['DATA_PATH'] = self.data_path
        os.environ['LOCATION_PATH'] = self.location_path
        os.environ['OUT_PATH'] = self.out_path
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['SOURCE_TYPE_INDEX'] = self.source_type_index
        os.environ['YEAR_INDEX'] = self.year_index
        os.environ['MONTH_INDEX'] = self.month_index
        os.environ['DAY_INDEX'] = self.day_index
        os.environ['FILE_INDEX'] = self.file_index
        app.main()
        self.check_output()

    def check_output(self):
        print(f'input_data_path: {self.input_data_path}')
        print(f'input_location_path: {self.input_location_path}')

        root_path = os.path.join(self.out_path, self.metadata_path, '00001')

        self.output_data_path = os.path.join(root_path, 'data', self.data_file)
        self.output_location_path = os.path.join(root_path, 'location', self.location_file)

        print(f'output_data_path: {self.output_data_path}')
        print(f'output_location_path: {self.output_location_path}')

        self.assertTrue(os.path.lexists(self.output_data_path))
        self.assertTrue(os.path.lexists(self.output_location_path))
