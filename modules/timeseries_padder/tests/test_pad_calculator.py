#!/usr/bin/env python3
import unittest
from timeseries_padder.timeseries_padder import pad_calculator
import datetime


class PadCalculatorTest(unittest.TestCase):

    def test_convert_window_size(self):
        self.assertEqual(pad_calculator.convert_window_size(10, 0.1), 100)
        self.assertEqual(pad_calculator.convert_window_size(50, 1), 50)
        with self.assertRaises(ValueError):
            pad_calculator.convert_window_size(10, 0)
            pad_calculator.convert_window_size(10, -1)
            pad_calculator.convert_window_size(-1, 0.1)

    def test_calculate_pad_size(self):
        self.assertEqual(pad_calculator.calculate_pad_size(0.001), 1)
        self.assertEqual(pad_calculator.calculate_pad_size(50), 1)
        self.assertEqual(pad_calculator.calculate_pad_size(86400), 1)
        self.assertEqual(pad_calculator.calculate_pad_size(86400.1), 2)
        self.assertEqual(pad_calculator.calculate_pad_size(172800), 2)
        self.assertEqual(pad_calculator.calculate_pad_size(172900), 3)
        with self.assertRaises(ValueError):
            pad_calculator.calculate_pad_size(-10)

    def test_get_padded_dates(self):
        date_range = [datetime.date(2018, 6, 14), datetime.date(2018, 6, 15), datetime.date(2018, 6, 16)]
        self.assertEqual(pad_calculator.get_padded_dates(datetime.date(2018, 6, 15), 1), date_range)
        date_range = [datetime.date(2018, 6, 14), datetime.date(2018, 6, 15), datetime.date(2018, 6, 16)]
        self.assertEqual(pad_calculator.get_padded_dates(datetime.date(2018, 6, 15), 0.1), date_range)
        date_range = [datetime.date(2017, 12, 29), datetime.date(2017, 12, 30),
                      datetime.date(2017, 12, 31), datetime.date(2018, 1, 1), datetime.date(2018, 1, 2)]
        self.assertEqual(pad_calculator.get_padded_dates(datetime.date(2017, 12, 31), 2), date_range)
        date_range = [datetime.date(2018, 6, 14)]
        self.assertEqual(pad_calculator.get_padded_dates(datetime.date(2018, 6, 14), 0), date_range)
        # test -1 for only preceding day
        expected_date_range = [datetime.date(2018, 6, 14), datetime.date(2018, 6, 15)]
        date_range = pad_calculator.get_padded_dates(datetime.date(2018, 6, 15), -1)
        self.assertEqual(expected_date_range, date_range)
        # test 1 for day on either side
        expected_date_range = [datetime.date(2018, 6, 14), datetime.date(2018, 6, 15), datetime.date(2018, 6, 16)]
        date_range = pad_calculator.get_padded_dates(datetime.date(2018, 6, 15), 1)
        self.assertEqual(expected_date_range, date_range)
