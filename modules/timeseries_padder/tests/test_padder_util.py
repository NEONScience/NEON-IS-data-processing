import unittest
from timeseries_padder.timeseries_padder import padder_util
import datetime


class TestPadderUtil(unittest.TestCase):

    def test_convertWindowSize(self):
        self.assertEqual(padder_util.convert_window_size(10, 0.1), 100)
        self.assertEqual(padder_util.convert_window_size(50, 1), 50)
        with self.assertRaises(ValueError):
            padder_util.convert_window_size(10, 0)
            padder_util.convert_window_size(10, -1)
            padder_util.convert_window_size(-1, 0.1)

    def test_calculatePadSize(self):
        self.assertEqual(padder_util.calculate_pad_size(0.001), 1)
        self.assertEqual(padder_util.calculate_pad_size(50), 1)
        self.assertEqual(padder_util.calculate_pad_size(86400), 1)
        self.assertEqual(padder_util.calculate_pad_size(86400.1), 2)
        self.assertEqual(padder_util.calculate_pad_size(172800), 2)
        self.assertEqual(padder_util.calculate_pad_size(172900), 3)
        with self.assertRaises(ValueError):
            padder_util.calculate_pad_size(-10)

    def test_getDatesInPaddedRange(self):
        date_range = [datetime.date(2018, 6, 14), datetime.date(2018, 6, 15), datetime.date(2018, 6, 16)]
        self.assertEqual(padder_util.get_dates_in_padded_range(datetime.date(2018, 6, 15), 1), date_range)
        date_range = [datetime.date(2018, 6, 14), datetime.date(2018, 6, 15), datetime.date(2018, 6, 16)]
        self.assertEqual(padder_util.get_dates_in_padded_range(datetime.date(2018, 6, 15), 0.1), date_range)
        date_range = [datetime.date(2017, 12, 29), datetime.date(2017, 12, 30),
                      datetime.date(2017, 12, 31), datetime.date(2018, 1, 1), datetime.date(2018, 1, 2)]
        self.assertEqual(padder_util.get_dates_in_padded_range(datetime.date(2017, 12, 31), 2), date_range)
        date_range = [datetime.date(2018, 6, 14)]
        self.assertEqual(padder_util.get_dates_in_padded_range(datetime.date(2018, 6, 14), 0), date_range)


if __name__ == '__main__':
    unittest.main()
