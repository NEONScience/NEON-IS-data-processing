#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

import parquet_linkmerge.app as app
from common import log_config as log_config


class ParquetLinkMergeTest(TestCase):

    def setUp(self):
        log_config.configure('DEBUG')

        self.setUpPyfakefs()

        self.out_path = Path('/repo/outputs')
        self.in_path = Path('/repo/inputs')
        self.metadata_path = Path('prt/2019/10/02')

        self.data_files = [
            'GRSM_prt_6974_2019-10-02.parquet',
            'UNDE_prt_6848_2019-10-02.parquet',
            'WREF_prt_6848_2019-10-02.parquet'
        ]

        self.expected_files = [
            'prt_6974_2019-10-02.parquet',
            'prt_6848_2019-10-02.parquet',
        ]

        for data_file in self.data_files:
            name_parts = data_file.split('_')
            source_id = name_parts[2]
            data_path = Path(self.in_path, self.metadata_path, source_id, data_file)
            # use real data file to convert
            actual_data_file_path = Path(os.path.dirname(__file__), data_file)
            self.fs.add_real_file(actual_data_file_path, target_path=data_path)

    def test_main(self):
        os.environ['IN_PATH'] = str(self.in_path)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        app.main()
        self.check_output()

    def check_output(self):
        for data_file in self.expected_files:
            name_parts = data_file.split('_')
            source_id = name_parts[1]
            data_path = Path(self.out_path, self.metadata_path, source_id, data_file)
            self.assertTrue(data_path.exists())
