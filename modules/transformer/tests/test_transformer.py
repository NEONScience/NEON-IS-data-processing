#!/usr/bin/env python3
import os
from pathlib import Path
from unittest import TestCase
from transformer.transformer import transform, format_column, format_sig
import transformer.transformer_main as transformer_main
from testfixtures import TempDirectory
import json
import pandas as pd
import datetime
import fnmatch

class TransformerTest(TestCase):

    def setUp(self):

        # create temporary dir
        self.temp_dir = TempDirectory()
        self.temp_dir_name = self.temp_dir.path
        self.input_path = Path(self.temp_dir_name, "repo/inputs")
        self.data_path = Path(self.input_path, "CPER/2019/05/24")
        self.out_path = Path(self.temp_dir_name, "outputs")
        self.output_path = Path(self.out_path, "CPER/2019/05/24")
        self.location = "CFGLOC123"
        self.workbook_path = Path(self.input_path, 'workbook.csv')
        self.location_path = Path(self.data_path, 'locations', self.location, 'location.json')
        self.data_file = Path(self.data_path, 'data', self.location, 'data_001.parquet')
        os.makedirs(Path(self.data_path, 'locations', self.location))
        os.makedirs(Path(self.data_path, 'data', self.location))
        self.year_index = 10

        # create workbook dataframe
        workbook = pd.DataFrame()
        workbook['fieldName'] = ['startDateTime', 'endDateTime', 'mean']
        workbook['DPNumber'] = 'dpnumber.001'
        workbook['downloadPkg'] = 'basic'
        workbook['rank'] = [1,2,3]
        workbook['table'] = 'table001'
        workbook['dpID'] = 'NEON.DOM.SITE.DP1.00041.001'
        workbook['pubFormat'] = ["yyyy-MM-dd'T'HH:mm:ss'Z'(floor)", '*.###(round)', 'asIs']
        # write workbook to csv
        workbook.to_csv(self.workbook_path, sep ='\t', index=False)

        # create location json
        locdata = {'type': 'FeatureCollection', 'features': [{'type': 'Feature', 'geometry': None, 'properties': {'name': 'CFGLOC101746', 'type': 'CONFIG', 'description': 'Central Plains Soil Temp Profile SP1, Z5 Depth', 'domain': 'D10', 'site': 'CPER', 'context': ['soil'], 'active_periods': [{'start_date': '2016-04-08T00:00:00Z'}]}, 'HOR': '001', 'VER': '505', 'TMI': '000', 'Data Rate': '0.1', 'Required Asset Management Location ID': 3095, 'Required Asset Management Location Code': 'CFGLOC101746'}]}
        # write location to json
        with open(self.location_path, 'w') as f:
            json.dump(locdata, f)

        # create data dataframe
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
        os.environ["WORKBOOK_PATH"] = str(self.workbook_path)
        os.environ["DATA_PATH"] = str(self.data_path)
        os.environ["OUT_PATH"] = str(self.out_path)
        transform(year_index=self.year_index)
        self.check_output()

    def test_main(self):
        os.environ["WORKBOOK_PATH"] = str(self.workbook_path)
        os.environ["DATA_PATH"] = str(self.data_path)
        os.environ["OUT_PATH"] = str(self.out_path)
        os.environ["YEAR_INDEX"] = str(self.year_index)
        transformer_main.main()
        self.check_output()

    def check_output(self):
        out_files = os.listdir(self.output_path)
        basic_pattern = 'NEON.D10.CPER.DP1.00041.001.001.505.001.table001.2019-05-24.basic.csv'
        self.assertTrue(len(out_files) == 1)
        self.assertTrue(fnmatch.fnmatch(out_files[0], basic_pattern))

    def tearDown(self):
        self.temp_dir.cleanup()
