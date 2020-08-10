#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

from path_order.path_order import order_paths
import path_order.path_order_main as path_order_main


class PathOrderTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.in_path = Path('/in/data')
        self.out_path = Path('/out')
        # set the source ID before the filename
        self.data_metadata_path = Path('prt/2019/07/23/0001')
        self.data_filename = 'prt_0001_2019-07-23.ext'
        data_path = Path(self.in_path, self.data_metadata_path, self.data_filename)
        self.fs.create_file(data_path)
        # path indices start at 3 to trim off the in_path and move the source ID
        # under the source type
        self.path_indices = "3, 7, 4, 5, 6, 8"

    def test_move_source_id(self):
        order_paths(self.in_path, self.out_path, self.path_indices.split(','))
        self.check_output()

    def test_reverse(self):
        """Reverse the path."""
        order_paths(self.in_path, self.out_path, [7, 6, 5, 4, 3, 8])
        expected_path = Path(self.out_path, '0001/23/07/2019/prt', self.data_filename)
        self.assertTrue(expected_path.exists())

    def test_main(self):
        os.environ['IN_PATH'] = str(self.in_path)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['PATH_INDICES'] = str(self.path_indices)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        path_order_main.main()
        self.check_output()

    def check_output(self):
        # confirm the source ID has moved under the source type in the path
        expected_path = Path(self.out_path, 'prt/0001/2019/07/23', self.data_filename)
        self.assertTrue(expected_path.exists())
