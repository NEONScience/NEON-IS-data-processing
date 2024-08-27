#!usr/bin/env python3
import os
import shutil
import unittest
import glob
from structlog import get_logger
from pathlib import Path
from unittest import TestCase
from raw_data_parser.raw_data_parser import parse_raw

log = get_logger()


class LoadRawParserTest(TestCase):

    def setUp(self):
        self.source_type = 'li7200_raw'
        self.parse_field = 'ethernet_output'
        self.data_path = Path('pfs/DATA_PATH')
        self.out_path = Path('pfs/OUT_PATH')
        self.relative_path_index: int = int('4')
        print(f'final output path is: {Path(self.out_path)}')

    def test_load_rawparser(self):
        # clean up the output directory left from previous testing
        if os.path.exists(self.out_path):
            shutil.rmtree(self.out_path)
        parse_raw(source_type=self.source_type, parse_field=self.parse_field, data_path=self.data_path,
                  out_path=self.out_path, relative_path_index=self.relative_path_index)
        for path in glob.glob(f'{self.out_path}/*/**/', recursive=True):
            print('path : ', path)
            self.assertTrue(Path(path).exists())


if __name__ == '__main__':
    unittest()
