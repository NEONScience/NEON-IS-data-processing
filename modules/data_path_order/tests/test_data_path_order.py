#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

from data_path_order.data_path_order_config import Config
from data_path_order.data_path_order import order_paths
import data_path_order.data_path_order_main as data_path_order_main


class DataPathOrderTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.in_path = Path('/in/data')
        self.out_path = Path('/out')
        self.data_metadata_path = Path('prt/2019/07/23')
        self.data_filename = 'prt_0001_2019-07-23.ext'
        data_path = Path(self.in_path, self.data_metadata_path, self.data_filename)
        self.fs.create_file(data_path)
        # path indices
        self.source_type_index = 3
        self.year_index = 4
        self.month_index = 5
        self.day_index = 6
        self.source_id_index = 7

    def test_group_files(self):
        config = Config(data_path=self.in_path,
                        out_path=self.out_path,
                        source_type_index=self.source_type_index,
                        year_index=self.year_index,
                        month_index=self.month_index,
                        day_index=self.day_index)
        order_paths(config)
        self.check_output()

    def test_main(self):
        os.environ['DATA_PATH'] = str(self.in_path)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['SOURCE_TYPE_INDEX'] = str(self.source_type_index)
        os.environ['YEAR_INDEX'] = str(self.year_index)
        os.environ['MONTH_INDEX'] = str(self.month_index)
        os.environ['DAY_INDEX'] = str(self.day_index)
        data_path_order_main.main()
        self.check_output()

    def check_output(self):
        expected_path = Path(self.out_path, 'prt/0001/2019/07/23', self.data_filename)
        self.assertTrue(expected_path.exists())
