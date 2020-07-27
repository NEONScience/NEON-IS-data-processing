#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

from common import log_config as log_config

from parquet_linkmerge import parquet_linkmerge_main
from parquet_linkmerge.parquet_file_merger import ParquetFileMerger


class ParquetLinkMergeTest(TestCase):

    def setUp(self):
        log_config.configure('DEBUG')
        self.setUpPyfakefs()
        self.in_path = Path('/dir/in')
        self.out_path = Path('/out')
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
            data_path = Path(self.in_path, self.metadata_path, data_file)
            # use real data files
            actual_data_file_path = Path(os.path.dirname(__file__), data_file)
            self.fs.add_real_file(actual_data_file_path, target_path=data_path)
        self.relative_path_index = 3

    def test_file_merger(self):
        file_merger = ParquetFileMerger(data_path=self.in_path, out_path=self.out_path,
                                        deduplication_threshold=0.3, relative_path_index=3)
        file_merger.merge()
        self.check_output()

    def test_main(self):
        os.environ['IN_PATH'] = str(self.in_path)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['RELATIVE_PATH_INDEX'] = str(self.relative_path_index)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        parquet_linkmerge_main.main()
        self.check_output()

    def check_output(self):
        for data_file in self.expected_files:
            source_id = data_file.split('_')[1]
            data_path = Path(self.out_path, 'prt', source_id, '2019/10/02', data_file)
            self.assertTrue(data_path.exists())
