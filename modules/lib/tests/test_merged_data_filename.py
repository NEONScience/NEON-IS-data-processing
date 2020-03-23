import unittest

from lib.merged_data_filename import MergedDataFilename


class TestDataFilename(unittest.TestCase):

    def setUp(self):
        filename = 'prt_CFGLOC112154_2018-01-03.ext'
        self.data_filename = MergedDataFilename(filename)

    def test_date(self):
        date = self.data_filename.date()
        self.assertTrue(date == '2018-01-03')

    def location(self):
        location = self.data_filename.location()
        self.assertTrue(location == 'CFGLOC112154')


if __name__ == '__main__':
    unittest.main()
