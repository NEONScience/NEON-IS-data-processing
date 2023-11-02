#!/usr/bin/env python3
import os
from pathlib import Path
from typing import List

import unittest
import geojson
import json

from data_access.tests.database_test import DatabaseBackedTest
from data_access.types.group import Group
from data_access.types.property import Property
from data_access.types.active_period import ActivePeriod
from group_loader import group_loader_main, group_loader
from common.date_formatter import to_datetime


class GroupLoaderTest(DatabaseBackedTest):

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = Path('/out')
        self.err_path = Path('/tmp/out/errored')
        self.fs.create_dir(self.out_path)

    @unittest.skip('Integration test skipped due to long process time.')
    def test_main(self):
        self.configure_mount()
        os.environ['GROUP_PREFIX'] = 'test-'
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['ERR_PATH'] = str(self.err_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        group_loader_main.main()
        file_path = Path('/out/test/rel-humidity_CPER000040/rel-humidity_CPER000040.json')
        self.assertTrue(file_path.exists())

    def test_group_loader(self):
        s_date = '2022-11-07T11:56:11Z'
        e_date = '2022-11-08T12:57:12Z'
        name = 'test-group_2'
        group_name = 'test-group_1'
        data_product_id = 'DP1.20008.001'
        groups = []

        def get_groups(group_prefix) -> List[Group]:
            """Mock function to return groups."""
            periods = [ActivePeriod(start_date=to_datetime(s_date), end_date=to_datetime(e_date))]
            props = [Property('HOR', '000'), Property('VER', '000')]
            groups.append(Group(name=name, group=group_name, active_periods=periods,
            data_product_ID=data_product_id, properties=props))
            print(f'[groups] in test_group_loader before return: {[groups]}')
            groups.append(groups)
            return [groups]

        # test the function
        group_loader.load_groups(out_path=self.out_path, err_path=self.err_path, get_groups=get_groups, group_prefix='test-')

        # check the output
        file_path = Path(self.out_path, 'test-', 'test-group_2', 'test-group_2.json')
        self.assertTrue(file_path.exists())
        with open(file_path) as file:
            file_data = geojson.load(file)
            geojson_data = geojson.dumps(file_data, indent=4, sort_keys=False, default=str)
            json_data = json.loads(geojson_data)
            features = json_data['features'][0]
            properties = json_data['features'][0]['properties']
            active_periods = properties['active_periods']
            period = active_periods[0]
            self.assertTrue(period['start_date'] == s_date)
            self.assertTrue(period['end_date'] == e_date)
            self.assertTrue(properties['name'] == 'test-group_2')
            self.assertTrue(properties['group'] == 'test-group_1')
            self.assertTrue(features['HOR'] == '000')
            self.assertTrue(features['VER'] == '000')
