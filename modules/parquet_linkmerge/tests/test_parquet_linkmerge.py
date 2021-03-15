#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

from common import log_config as log_config

from parquet_linkmerge import parquet_linkmerge_main
from parquet_linkmerge.parquet_file_merger import ParquetFileMerger
from parquet_linkmerge.parquet_linkmerge_config import Config


class ParquetLinkMergeTest(TestCase):

    def setUp(self):
        log_config.configure('DEBUG')
        self.setUpPyfakefs()
        self.in_path = Path('/dir/in')
        self.out_path = Path('/out')
        self.metadata_path = Path('prt/2019/10')

        file_name_1 = 'GRSM_prt_6974_2019-10-02.parquet'
        file_name_2 = 'UNDE_prt_6848_2019-10-02.parquet'
        file_name_3 = 'WREF_prt_6848_2019-10-02.parquet'
        file_name_4 = 'CPER_prt_6848_2019-10-03.parquet'

        data_path_1 = Path(self.in_path, self.metadata_path, '02/6974/data', file_name_1)
        actual_data_file_path = Path(os.path.dirname(__file__), file_name_1)
        self.fs.add_real_file(actual_data_file_path, target_path=data_path_1)

        data_path_2 = Path(self.in_path, self.metadata_path, '02/6848/data', file_name_2)
        actual_data_file_path = Path(os.path.dirname(__file__), file_name_2)
        self.fs.add_real_file(actual_data_file_path, target_path=data_path_2)

        data_path_3 = Path(self.in_path, self.metadata_path, '02/6848/data', file_name_3)
        actual_data_file_path = Path(os.path.dirname(__file__), file_name_3)
        self.fs.add_real_file(actual_data_file_path, target_path=data_path_3)

        data_path_4 = Path(self.in_path, self.metadata_path, '03/6848/data', file_name_4)
        actual_data_file_path = Path(os.path.dirname(__file__), file_name_4)
        self.fs.add_real_file(actual_data_file_path, target_path=data_path_4)

    def test_file_merger(self):
        config = Config(in_path=self.in_path,
                        out_path=self.out_path,
                        duplication_threshold=0.3,
                        source_type_index=3,
                        year_index=4,
                        month_index=5,
                        day_index=6,
                        source_id_index=7)
        file_merger = ParquetFileMerger(config)
        file_merger.merge()
        self.check_output()

    def test_main(self):
        os.environ['IN_PATH'] = str(self.in_path)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['SOURCE_TYPE_INDEX'] = str(3)
        os.environ['YEAR_INDEX'] = str(4)
        os.environ['MONTH_INDEX'] = str(5)
        os.environ['DAY_INDEX'] = str(6)
        os.environ['SOURCE_ID_INDEX'] = str(7)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        parquet_linkmerge_main.main()
        self.check_output()

    def check_output(self):
        self.assertTrue(Path(self.out_path, self.metadata_path, '02/6974/data/prt_6974_2019-10-02.parquet').exists())
        self.assertTrue(Path(self.out_path, self.metadata_path, '02/6848/data/prt_6848_2019-10-02.parquet').exists())
        self.assertTrue(Path(self.out_path, self.metadata_path, '03/6848/data/prt_6848_2019-10-03.parquet').exists())
