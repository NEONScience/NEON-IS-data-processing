#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

from grouper.grouper import group_files
import grouper.grouper_main as grouper_main


class GrouperTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.input_path = Path('/repo/inputs')
        self.output_path = Path('/outputs')
        self.group = 'aspirated-single-121'
        self.location = 'CFGLOC123'
        self.metadata_path = Path('2019/05/24', self.group)
        self.data_dir = 'data'
        self.location_dir = 'location'
        self.data_file = 'data.ext'
        self.location_file = 'locations.json'
        self.base_path = Path(self.input_path, 'prt', self.metadata_path)
        self.in_data_path = Path(self.base_path, self.location, self.data_dir, self.data_file)
        self.in_location_path = Path(self.base_path, self.location, self.location_dir, self.location_file)
        self.fs.create_file(self.in_data_path)
        self.fs.create_file(self.in_location_path)
        self.relative_path_index = 3

    def test_group(self):
        group_files(path=self.base_path, out_path=self.output_path, relative_path_index=self.relative_path_index)
        self.check_output()

    def test_main(self):
        os.environ['DATA_PATH'] = str(self.base_path)
        os.environ['OUT_PATH'] = str(self.output_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['RELATIVE_PATH_INDEX'] = str(self.relative_path_index)
        grouper_main.main()
        self.check_output()

    def check_output(self):
        root_path = Path(self.output_path, 'prt', self.metadata_path, self.location)
        out_data_path = Path(root_path, self.data_dir, self.data_file)
        out_location_path = Path(root_path, self.location_dir, self.location_file)
        self.assertTrue(out_data_path.exists())
        self.assertTrue(out_location_path.exists())
