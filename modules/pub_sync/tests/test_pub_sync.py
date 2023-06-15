#!/usr/bin/env python3
import datetime
import fnmatch
import glob
import json
import logging
import os
import sys
from pathlib import Path
from typing import List
import unittest

import pandas as pd
from testfixtures import TempDirectory
from data_access.tests.database_test import DatabaseBackedTest
from data_access.db_connector import DbConnector
from data_access.get_dp_pub_records import get_dp_pub_records
from data_access.remove_pub import remove_pub

from pub_sync import pub_sync_main, pub_sync


class PubSyncTest(DatabaseBackedTest):
#class PubSyncTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = Path('/out')
        self.fs.create_dir(self.out_path)

    @unittest.skip('Integration test skipped due to long process time.')
    def test_pub_sync(self):
        temp_dir = TempDirectory()
        temp_dir_name = temp_dir.path
        date_path_year_index = "3"
        date_path_month_index = "4"
        data_path_product_index = "3"
        data_site_index = "4"
        data_path_date_index = "5"
        data_path_package_index = "6"
        dp_ids: List[str] = ["NEON.DOM.SITE.DP1.00066.001"]
        sites: List[str] = ["CPER","HARV","ABBY"]
        change_by = "pachyderm"

        pub_sync(connector = connector,
             data_path = self.data_path,
             date_path_year_index = self.date_path_year_index,
             date_path_month_index = self.date_path_month_index,
             data_path_product_index = self.data_path_product_index,
             data_path_site_index = self.data_path_site_index,
             data_path_date_index = self.data_path_date_index,
             data_path_package_index = self.data_path_package_index,
             dp_ids = self.dp_ids,
             sites = self.sites,
             change_by = self.change_by)
        self.check_output()


    def check_output(self):
        os.chdir(self.output_path)
        out_files = glob.glob("*.csv")
        print("NUMBER OF OUTPUT FILES = " + str(len(out_files)))
        basic_pattern = 'NEON.D10.CPER.DP1.00066.001.001.000.001.table001.2019-05-24.basic.csv'
        self.assertTrue(len(out_files) == 2)
        self.assertTrue(fnmatch.fnmatch(out_files[0], basic_pattern) | fnmatch.fnmatch(out_files[1], basic_pattern))

    def tearDown(self):
        self.temp_dir.cleanup()


