#!/usr/bin/env python3
import os
import glob
import sys
from pathlib import Path
from unittest import TestCase
from pub_packager.pub_packager import pub_package
import pub_packager.pub_packager_main as pub_packager_main
from testfixtures import TempDirectory
import fnmatch
import logging


class Pub_packager(TestCase):

    def setUp(self):
        self.log = logging.getLogger('testlog')
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
        self.temp_dir = TempDirectory()  # generate_readme temporary dir
        self.temp_dir_name = self.temp_dir.path
        self.input_path = Path(self.temp_dir_name, 'repo/inputs')
        self.data_path = Path(self.input_path, '2019/05')
        self.out_path = Path(self.temp_dir_name, 'outputs')
        self.output_path = Path(self.out_path, 'CPER/2019/05')
        self.data_file_1 = Path(self.data_path,
                                '24/CPER/NEON.D10.CPER.DP1.00066.001.001.000.001.ST_1_minute.2019-05-24.basic.csv')
        self.data_file_2 = Path(self.data_path,
                                '25/CPER/NEON.D10.CPER.DP1.00066.001.001.000.001.ST_1_minute.2019-05-25.basic.csv')
        self.manifest_file_1 = Path(self.data_path,'24/CPER/manifest.csv')
        self.manifest_file_2 = Path(self.data_path,'25/CPER/manifest.csv')
        os.makedirs(Path(self.data_path, '24','CPER'))
        os.makedirs(Path(self.data_path, '25','CPER'))
        with open(self.data_file_1, 'w') as f:
            f.write('file 1 content')
        with open(self.data_file_2, 'w') as f:
            f.write('file 2 content')
        with open(self.manifest_file_1, 'w') as f:
            f.write('file,hasData,visibility\n')
            f.write('NEON.D10.CPER.DP1.00066.001.001.000.001.ST_1_minute.2019-05-24.basic.csv,True,public')
        with open(self.manifest_file_2, 'w') as f:
            f.write('file,hasData,visibility\n')
            f.write('NEON.D10.CPER.DP1.00066.001.001.000.001.ST_1_minute.2019-05-25.basic.csv,True,public')
        self.publoc_index = self.data_file_1.parts.index('CPER')
        self.date_index = self.data_file_1.parts.index('2019')
        self.date_index_length = 2
        self.sort_index = 10

    def test_package(self):
        pub_package(data_path=self.data_path,
                out_path=self.out_path,
                publoc_index=self.publoc_index,
                date_index=self.date_index,
                date_index_length=self.date_index_length,
                sort_index=self.sort_index)
        self.check_output()

    def test_main(self):
        os.environ['DATA_PATH'] = str(self.data_path)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['PUBLOC_INDEX'] = str(self.publoc_index)
        os.environ['DATE_INDEX'] = str(self.date_index)
        os.environ['DATE_INDEX_LENGTH'] = str(self.date_index_length)
        os.environ['SORT_INDEX'] = str(self.sort_index)
        os.environ['LOG_LEVEL'] = "DEBUG"
        pub_packager_main.main()
        self.check_output()

    def check_output(self):
        os.chdir(self.output_path)
        out_files = glob.glob('*.csv')
        self.log.debug(f'INPUT PATH = {str(self.input_path)}')
        self.log.debug(f'OUTPUT PATH = {str(self.out_path)}')
        self.log.debug(f'NUMBER OF OUTPUT FILES {str(len(out_files))}')
        self.log.debug(f'OUTPUT FILES {str(out_files)}')
        basic_pattern = 'NEON.D10.CPER.DP1.00066.001.001.000.001.ST_1_minute.2019-05.basic.*.csv'
        self.assertTrue(len(out_files) == 2)
        self.assertTrue(fnmatch.fnmatch(out_files[0], basic_pattern))

    def tearDown(self):
        self.temp_dir.cleanup()
