#!/usr/bin/env python3
import datetime
import fnmatch
import glob
import json
import logging
import os
import sys
from pathlib import Path
from typing import List, Dict
import unittest

import pandas as pd
from testfixtures import TempDirectory
from data_access.tests.database_test import DatabaseBackedTest
from data_access.db_connector import DbConnector
from data_access.get_dp_pub_records import get_dp_pub_records
from data_access.types.dp_pub import DpPub
from data_access.remove_pub import remove_pub
from common.get_path_key import get_path_key

from pub_sync import pub_sync_main, pub_sync

class PubSyncTest(DatabaseBackedTest):
#class PubSyncTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = Path('/out')
        self.fs.create_dir(self.out_path)


    def test_pub_sync(self):
        temp_dir = TempDirectory()
        temp_dir_name = temp_dir.path
        date_path_year_index = "3"
        date_path_month_index = "4"
        data_path_product_index = "3"
        data_path_site_index = "4"
        data_path_date_index = "5"
        data_path_package_index = "6"
        dp_ids: List[str] = ["NEON.DOM.SITE.DP1.00066.001"]
        sites: List[str] = ["CPER","HARV","ABBY"]
        change_by = "pachyderm"
        data_path = None
        date_path = Path('2023/04/01')

        def get_sync_pubs(pub_dates: List[Dict], dp_ids: List[str], sites: List[str], psmp_pachy: List[Dict]) -> List[DpPub]:
            """Mock function to return groups."""
            psmp_portal_remove = {}
            pub_dates = {}
            date_key = '202304'
            data_interval_end = 1
            cutoff_date = '202305'
            pub_dates[date_key] = [date_key + '01T00:00:00Z',
                                   cutoff_date + '01T00:00:00Z']



        pub_sync.sync_pubs(get_sync_pubs = get_sync_pubs,
                           data_path=data_path,
                           date_path=date_path,
                           date_path_year_index=date_path_year_index,
                           date_path_month_index=date_path_month_index,
                           data_path_product_index=data_path_product_index,
                           data_path_site_index=data_path_site_index,
                           data_path_date_index=data_path_date_index,
                           data_path_package_index=data_path_package_index,
                           dp_ids=dp_ids,
                           sites=sites,
                           change_by=change_by)

        self.assertTrue(change_by == 'pachyderm')

