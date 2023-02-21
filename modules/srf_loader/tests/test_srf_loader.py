#!/usr/bin/env python3
import os
from pathlib import Path
from typing import List

import unittest
import geojson
import json

from data_access.tests.database_test import DatabaseBackedTest
from data_access.types.srf import Srf
import data_access.types.geojson_converter as geojson_converter
from data_access.types.property import Property
from data_access.types.active_period import ActivePeriod
import srf_loader.srf_loader as srf_loader
import srf_loader.srf_loader_main as srf_loader_main

from common.date_formatter import to_datetime


class SrfLoaderTest(DatabaseBackedTest):

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = Path('/out')
        self.fs.create_dir(self.out_path)

    @unittest.skip('Integration test skipped due to long process time.')
    def test_main(self):
        self.configure_mount()
        os.environ['GROUP_PREFIX'] = 'test-'
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        srf_loader_main.main()
        # file_path = Path('/out/test/rel-humidity_CPER000040/rel-humidity_CPER000040.json')
        # self.assertTrue(file_path.exists())

    def test_srf_loader(self):
        s_date = '2022-11-07T11:56:11Z'
        e_date = '2022-11-08T12:57:12Z'
        data_product_id = 'DP1.20008.001'
        srf: List[Srf] = []
        srfs = []

        def get_srfs(group_prefix) -> List[Srf]:
            print(f'prefix: {group_prefix}')
            """Mock function to return srfs."""
            periods = [ActivePeriod(start_date=to_datetime(s_date), end_date=to_datetime(e_date))]
            props = [Property('HOR', '000'), Property('VER', '000')]
            srf.append(Group(name='test-group_2', group='test-group_1', active_periods=periods, 
            data_product_ID=data_product_id, properties=props))
            srfs.append(srf)
            return groups

        # test the function
        srf_loader.load_srfs(out_path=self.out_path, get_srfs=get_srfs, group_prefix='test-')

        # check the output
        file_path = Path(self.out_path, 'test-', 'test-group_2', 'test-group_2.json')
        self.assertTrue(file_path.exists())
        geojson_data = geojson_converter.convert_group(groups)
        features = geojson_data['features']
        print(f'======== geojson_data:\n{geojson_data}')
        properties = geojson_data['features']['properties']
        active_periods = properties['active_periods']
        period = active_periods[0]
        self.assertTrue(period['start_date'] == s_date)
        self.assertTrue(period['end_date'] == e_date)
        self.assertTrue(properties['name'] == 'test-group_2')
        self.assertTrue(properties['group'] == 'test-group_1')
        self.assertTrue(properties['data_product_ID'] == data_product_id)
        self.assertTrue(features['HOR'] == '000')
        self.assertTrue(features['VER'] == '000')
