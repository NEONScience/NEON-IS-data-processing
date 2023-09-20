#!/usr/bin/env python3
import datetime
import fnmatch
import glob
import json
import logging
import os
import sys
from pathlib import Path
from unittest import TestCase

import pandas as pd
from testfixtures import TempDirectory

import pub_transformer.pub_transformer_main as pub_transformer_main
from pub_transformer.tests.group_data import get_group_data
from pub_transformer.pub_transformer import pub_transform, format_column, format_sig


class PubTransformerTest(TestCase):

    def setUp(self):

        self.temp_dir = TempDirectory()
        self.temp_dir_name = self.temp_dir.path
        self.input_path = Path(self.temp_dir_name, "repo/inputs")
        self.data_path = Path(self.input_path, "DP1.00066.001/2019/05/24/CPER")
        self.out_path = Path(self.temp_dir_name, "outputs")
        self.output_path = Path(self.out_path, "DP1.00066.001/2019/05/24/CPER")
        self.group = "par-quantum-line_CPER001000"
        self.workbook_path = Path(self.input_path, 'workbooks')
        self.workbook_file_path = Path(self.workbook_path, 'workbook.csv')
        self.group_path = Path(self.data_path, 'group', self.group, 'group.json')
        self.data_file = Path(self.data_path, 'data', self.group, 'data_table001.parquet')
        os.makedirs(self.workbook_path)
        os.makedirs(Path(self.data_path, 'group', self.group))
        os.makedirs(Path(self.data_path, 'data', self.group))
        self.product_index = self.data_file.parts.index("DP1.00066.001")
        self.year_index = self.data_path.parts.index("2019")
        self.data_type_index = self.data_file.parts.index("data")
        self.group_metadata_dir = "group"
        self.data_path_parse_index = self.product_index
        
        # generate_readme workbook dataframe
        workbook = pd.DataFrame()
        workbook['fieldName'] = ['startDateTime', 'endDateTime', 'mean']
        workbook['DPNumber'] = 'NEON.DOM.SITE.DP1.00066.001.TERMS.HOR.VER.001'
        workbook['downloadPkg'] = 'basic'
        workbook['rank'] = [1,2,3]
        workbook['table'] = 'table001'
        workbook['dpID'] = 'NEON.DOM.SITE.DP1.00066.001'
        workbook['pubFormat'] = ["yyyy-MM-dd'T'HH:mm:ss'Z'(floor)", '*.###(round)', 'asIs']
        workbook['dataCategory'] = 'Y'
        # write workbook to csv
        workbook.to_csv(self.workbook_file_path, sep ='\t', index=False)

        # write group to json
        with open(self.group_path, 'w') as f:
            json.dump(get_group_data(), f)

        # generate_readme data dataframe
        self.data = pd.DataFrame()
        start_times = [1546473660, 1546473720, 1546473780]
        end_times = [1546473720, 1546473780, 1546473840]
        self.data['startDateTime'] = [datetime.datetime.utcfromtimestamp(time) for time in start_times]
        self.data['endDateTime'] = [datetime.datetime.utcfromtimestamp(time) for time in end_times]
        self.data['mean'] = [0.123, 0.456, 0.789]
        # Keep a copy of the original dataframe to restore after tests modify it
        self.orig_data = self.data.copy()
        # write data to parquet
        self.data.to_parquet(self.data_file)

    def test_format_sig(self):
        formatted = format_sig(1234, 2)
        self.assertEqual(formatted, '1200')
        formatted = format_sig(0.0345123, 3)
        self.assertEqual(formatted, '0.0345')
        formatted = format_sig(2.34e-11, 3)
        self.assertEqual(formatted, '0.0000000')
        formatted = format_sig(2.34e-7, 3)
        self.assertEqual(formatted, '0.0000002')

    def test_format_column(self):
        format_column(self.data, 'startDateTime', "yyyy-MM-dd'T'HH:mm:ss'Z'(floor)")
        self.assertEqual(self.data['startDateTime'][0], '2019-01-03T00:01:00Z')
        format_column(self.data, 'mean', "*.##(round)")
        self.assertEqual(self.data['mean'][0], '0.12')
        self.data = self.orig_data.copy()
        format_column(self.data, 'mean', "signif_#(round)")
        self.assertEqual(self.data['mean'][2], '0.8')
        self.data = self.orig_data.copy()
        format_column(self.data, 'mean', "integer")
        self.assertEqual(self.data['mean'][2], '1')
        self.data = self.orig_data.copy()
        format_column(self.data, 'mean', "asIs")
        self.assertEqual(self.data['mean'][2], 0.789)
        return

    def test_transform(self):
        pub_transform(data_path=self.data_path,
                      out_path=self.out_path,
                      workbook_path=self.workbook_path,
                      product_index=self.product_index,
                      year_index=self.year_index,
                      data_type_index=self.data_type_index,
                      group_metadata_dir=self.group_metadata_dir,
                      data_path_parse_index=self.data_path_parse_index)
        self.check_output()

    def test_main(self):
        os.environ["LOG_LEVEL"] = "DEBUG"
        os.environ["WORKBOOK_PATH"] = str(self.workbook_path)
        os.environ["DATA_PATH"] = str(self.data_path)
        os.environ["OUT_PATH"] = str(self.out_path)
        os.environ["PRODUCT_INDEX"] = str(self.product_index)
        os.environ["YEAR_INDEX"] = str(self.year_index)
        os.environ["DATA_TYPE_INDEX"] = str(self.data_type_index)
        os.environ["GROUP_METADATA_DIR"] = str(self.group_metadata_dir)
        os.environ["DATA_PATH_PARSE_INDEX"] = str(self.data_path_parse_index)
        pub_transformer_main.main()
        self.check_output()

    def check_output(self):
        os.chdir(self.output_path)
        out_files = glob.glob("*.csv")
        print("NUMBER OF OUTPUT FILES = " + str(len(out_files)))
        basic_pattern = 'NEON.D10.CPER.DP1.00066.001.001.000.001.table001.2019-05-24.basic.csv'
        self.assertTrue(len(out_files) == 2)
        self.assertTrue(fnmatch.fnmatch(out_files[0], basic_pattern) | fnmatch.fnmatch(out_files[1], basic_pattern))
        #reset the directory
        os.chdir('/')

    def tearDown(self):
        self.temp_dir.cleanup()
