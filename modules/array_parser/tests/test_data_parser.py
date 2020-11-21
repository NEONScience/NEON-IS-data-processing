#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

import array_parser.data_file_parser as data_file_parser


class DataParserTest(TestCase):

    def setUp(self) -> None:
        self.data_file_path = Path(os.path.dirname(__file__), 'tchain_32610_2019-01-12.parquet')
        self.schema_file_path = Path(os.path.dirname(__file__), 'tchain_parsed.avsc')
        self.out_path = Path(os.path.dirname(__file__), 'output')

    def test_data_parser(self) -> None:
        data_file_parser.write_restructured_file(self.data_file_path, self.out_path, self.schema_file_path)
        Path(self.out_path, self.data_file_path.name).unlink()
