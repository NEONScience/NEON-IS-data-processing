#!/usr/bin/env python3
import unittest

from lib.data_filename import DataFilename


class DataFilenameTest(unittest.TestCase):

    def setUp(self):
        filename = 'prt_769_2018-01-03.extension'
        self.data_filename = DataFilename(filename)

    def test_source_type(self):
        source_type = self.data_filename.source_type()
        self.assertTrue(source_type == 'prt')

    def test_source_id(self):
        source_id = self.data_filename.source_id()
        self.assertTrue(source_id == '769')

    def test_date(self):
        date = self.data_filename.date()
        self.assertTrue(date == '2018-01-03')


if __name__ == '__main__':
    unittest.main()
