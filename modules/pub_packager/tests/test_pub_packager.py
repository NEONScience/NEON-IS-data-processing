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


class PubPackagerTest(TestCase):

    def setUp(self):
        self.log = logging.getLogger('testlog')
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
        self.temp_dir = TempDirectory()  # generate_readme temporary dir
        self.temp_dir_name = self.temp_dir.path
        self.input_path = Path(self.temp_dir_name, 'repo/inputs')
        self.data_path = Path(self.input_path, 'DP1.00066.001/2019/05')
        self.out_path = Path(self.temp_dir_name, 'outputs')
        self.output_path = Path(self.out_path, 'DP1.00066.001/CPER/2019/05')
        self.data_file_1 = Path(self.data_path,
                                '24/CPER/NEON.D10.CPER.DP1.00066.001.001.000.001.ST_1_minute.2019-05-24.basic.csv')
        self.data_file_2 = Path(self.data_path,
                                '25/CPER/NEON.D10.CPER.DP1.00066.001.001.000.001.ST_1_minute.2019-05-25.basic.csv')
        self.manifest_file_1 = Path(self.data_path,'24/CPER/manifest.csv')
        self.manifest_file_2 = Path(self.data_path,'25/CPER/manifest.csv')
        os.makedirs(Path(self.data_path, '24','CPER'))
        os.makedirs(Path(self.data_path, '25','CPER'))
        with open(self.data_file_1, 'w') as f:
            f.write('2019-05-24T00:00:00Z,2019-05-24T00:01:00Z,27.32,27.316,27.324,9.7e-06,6,0.1483,0.001271,0\n')
            f.write('2019-05-24T00:01:00Z,2019-05-24T00:02:00Z,24.245,24.239,24.249,1.25e-05,6,0.1469,0.001443,0')
            f.close()
        with open(self.data_file_2, 'w') as f:
            f.write('2022-07-02T01:14:00Z,2022-07-02T01:15:00Z,30.582,30.553,30.61,0.0004623,6,0.1508,0.008778,0\n')
            f.write('2022-07-02T01:13:00Z,2022-07-02T01:14:00Z,30.648,30.62,30.677,0.0004553,6,0.1509,0.008711,0\n')
            f.write('2022-07-02T01:15:00Z,2022-07-02T01:16:00Z,30.514,30.487,30.542,0.0004194,6,0.1506,0.00836,0')
            f.close()
        with open(self.manifest_file_1, 'w') as f:
            f.write('file,hasData,visibility\n')
            f.write('NEON.D10.CPER.DP1.00066.001.001.000.001.ST_1_minute.2019-05-24.basic.csv,True,public')
            f.close()
        with open(self.manifest_file_2, 'w') as f:
            f.write('file,hasData,visibility\n')
            f.write('NEON.D10.CPER.DP1.00066.001.001.000.001.ST_1_minute.2019-05-25.basic.csv,True,public')
            f.close()
        self.product_index = self.data_file_1.parts.index('DP1.00066.001')
        self.publoc_index = self.data_file_1.parts.index('CPER')
        self.date_index = self.data_file_1.parts.index('2019')
        self.date_index_length = 2
        self.sort_index = 10

    def test_package(self):
        pub_package(data_path=self.data_path,
                out_path=self.out_path,
                product_index=self.product_index,
                publoc_index=self.publoc_index,
                date_index=self.date_index,
                date_index_length=self.date_index_length,
                sort_index=self.sort_index)
        self.check_output()

    def test_main(self):
        os.environ['DATA_PATH'] = str(self.data_path)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['PRODUCT_INDEX'] = str(self.product_index)
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
        manifest = 'manifest*.csv'
        self.assertTrue(len(out_files) == 2)
        self.assertTrue(fnmatch.fnmatch(out_files[0],manifest))
        self.assertTrue(fnmatch.fnmatch(out_files[1], basic_pattern))
    #
    # def tearDown(self):
    #     self.temp_dir.cleanup()
