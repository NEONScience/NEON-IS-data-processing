#!/usr/bin/env python3
import os
import json
from pathlib import Path
from typing import Iterator

import unittest

from data_access.tests.database_test import DatabaseBackedTest
from data_access.types.srf import Srf
import srf_loader.srf_loader as srf_loader
import srf_loader.srf_loader_main as srf_loader_main


class SrfLoaderTest(DatabaseBackedTest):

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = Path('/out')
        self.fs.create_dir(self.out_path)

    def test_srf_loader(self):
        srf_name: str = "_science_review_flags"
        group_name = 'test-group_1'

        def get_srfs(group_prefix) -> Iterator[Srf]:
            """
            Mock function for getting srfs.
            :return: A srf.
            """
            yield Srf(group_name='test-group_1',
                      id = 123,
                      start_date = '2022-11-07T11:56:11Z',
                      end_date = '2022-11-08T12:57:12Z',
                      measurement_stream_name = 'meas_strm_name',
                      srf_term_name = 'srfTermName',
                      srf = 1,
                      user_comment = 'abc comments',
                      create_date = '2022-01-07T11:56:11Z',
                      last_update_date = '2023-01-08T12:57:12Z')

        srf_loader.load_srfs(out_path=self.out_path, get_srfs=get_srfs, group_prefix='test-')
        file_name = (group_name + srf_name + ".json")
        file_path = Path(self.out_path,group_name, file_name)

        # check the output
        self.assertTrue(file_path.exists())
        with open(file_path) as file:
            json_data = json.load(file)
            srfs = json_data['science_review_flags'][0]
            group_name: str = srfs['group_name']
            id: int = srfs['id']
            start_date = srfs['start_date']
            end_date = srfs['end_date']
            measurement_stream_name: str = srfs['measurement_stream_name']
            srf_term_name: str = srfs['srf_term_name']
            srf:int = srfs['srf']
            user_comment:str = srfs['user_comment']
            create_date = srfs['create_date']
            last_update_date = srfs['last_update_date']
            self.assertTrue(id == 123)
            self.assertTrue(group_name == 'test-group_1')
            self.assertTrue(start_date == '2022-11-07T11:56:11Z')
            self.assertTrue(end_date == '2022-11-08T12:57:12Z')
            self.assertTrue(measurement_stream_name == 'meas_strm_name')
            self.assertTrue(srf_term_name == 'srfTermName')
            self.assertTrue(srf == 1)
            self.assertTrue(user_comment == 'abc comments')
            self.assertTrue(create_date == '2022-01-07T11:56:11Z')
            self.assertTrue(last_update_date == '2023-01-08T12:57:12Z')
            print(json.dumps(json_data, indent=2, sort_keys=False))

    @unittest.skip('Integration test skipped due to long process time.')
    def test_main(self):
        self.configure_mount()
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        srf_loader_main.main()
        file_path = Path(self.out_path, '_science_review_flags.json')
        self.assertTrue(file_path.exists())

