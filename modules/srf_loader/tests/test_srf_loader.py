#!/usr/bin/env python3
import os
from pathlib import Path
from typing import List

import unittest
import geojson
import json

from data_access.tests.database_test import DatabaseBackedTest
from data_access.types.srf import Srf
from data_access.types.property import Property
from data_access.types.active_period import ActivePeriod

import data_access.types.geojson_converter as geojson_converter
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
        group = 'test-'
        id1 = 123
        s_date = '2022-11-07T11:56:11Z'
        e_date = '2022-11-08T12:57:12Z'
        m_stream_name = 'meas_strm_name'
        s_term_name = 'srfTermName'
        srflag = 1
        u_comment = 'abc comments'
        c_date = '2022-01-07T11:56:11Z'
        l_date = '2023-01-08T12:57:12Z'
        srf: List[Srf] = []
        srfs = []

        def get_srfs(group_prefix) -> List[Srf]:
            srf.append(Srf(group_name=group,id=id1,start_date=to_datetime(s_date),
                           end_date=to_datetime(e_date),measurement_stream_name=m_stream_name,
                           srf_term_name=s_term_name,srf=srflag,user_comment=u_comment,
                           create_date=to_datetime(c_date),last_update_date=to_datetime(l_date))
            srfs.append(srf)
            return srfs

        # test the function
        srf_loader.load_srfs(out_path=self.out_path,get_srfs=get_srfs,group_prefix='test-')

        # check the output
        file_path = Path(self.out_path,'test-','test-group_2','_science_review_floags.json')
        print(f'======== file_path:\n{file_path}')
        #     self.assertTrue(file_path.exists())
        #     geojson_data = geojson_converter.convert_group(groups)
        #     features = geojson_data['features']
        print(f'======== srfs:\n{srfs}')
        print(f'======== srf:\n{srf}')
        #     properties = geojson_data['features']['properties']
        #     active_periods = properties['active_periods']
        self.assertTrue(srf['start_date'] == s_date)
        self.assertTrue(srf['end_date'] == e_date)
