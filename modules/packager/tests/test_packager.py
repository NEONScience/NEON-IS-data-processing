#!/usr/bin/env python3
import os
from pathlib import Path
from unittest import TestCase
from packager.packager import package
import packager.packager_main as packager_main
from testfixtures import TempDirectory
import fnmatch

class Packager(TestCase):

    def setUp(self):

        # create temporary dir
        self.temp_dir = TempDirectory()
        self.temp_dir_name = self.temp_dir.path
        self.input_path = Path(self.temp_dir_name, "repo/inputs")
        self.data_path = Path(self.input_path, "CPER/2019/05")
        self.out_path = Path(self.temp_dir_name, "outputs")
        self.output_path = Path(self.out_path, "CPER/2019/05")
        self.data_file_1 = Path(self.data_path, '24/NEON.D10.CPER.DP1.00041.001.001.501.001.ST_1_minute.2019-05-24.basic.csv')
        self.data_file_2 = Path(self.data_path, '25/NEON.D10.CPER.DP1.00041.001.001.501.001.ST_1_minute.2019-05-25.basic.csv')
        os.makedirs(Path(self.data_path, '24'))
        os.makedirs(Path(self.data_path, '25'))
        with open(self.data_file_1, 'w') as f:
            f.write("file 1 content")
        with open(self.data_file_2, 'w') as f:
            f.write("file 2 content")
        self.prefix_index = self.data_path.parts.index("CPER")
        self.prefix_length = 3
        self.sort_index = self.data_path.parts.index("2019")

    def test_package(self):
        os.environ["DATA_PATH"] = str(self.data_path)
        os.environ["OUT_PATH"] = str(self.out_path)
        package(prefix_index=self.prefix_index, prefix_length=self.prefix_length, sort_index=self.sort_index)
        self.check_output()

    def test_main(self):
        os.environ["DATA_PATH"] = str(self.data_path)
        os.environ["OUT_PATH"] = str(self.out_path)
        os.environ["PREFIX_INDEX"] = str(self.prefix_index)
        os.environ["PREFIX_LENGTH"] = str(self.prefix_length)
        os.environ["SORT_INDEX"] = str(self.sort_index)
        packager_main.main()
        self.check_output()

    def check_output(self):
        out_files = os.listdir(self.output_path)
        basic_pattern = 'NEON.D10.CPER.DP1.00041.001.001.501.001.ST_1_minute.2019-05.basic.*.csv'
        self.assertTrue(len(out_files) == 1)
        self.assertTrue(fnmatch.fnmatch(out_files[0], basic_pattern))

    def tearDown(self):
        self.temp_dir.cleanup()
