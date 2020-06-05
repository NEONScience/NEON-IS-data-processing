#!/usr/bin/env python3
import unittest
from datetime import datetime, date
from pathlib import Path

import common.date_formatter as date_formatter


class DateFormatterTest(unittest.TestCase):

    def test_date_formatter(self):
        expected_date_time = '2019-01-01T00:00:00Z'
        date_time = '2019-01-01 00:00:00'
        date_time_format = '%Y-%m-%d %H:%M:%S'
        converted_date = datetime.strptime(date_time, date_time_format)
        formatted = date_formatter.convert(converted_date)
        self.assertEqual(expected_date_time, formatted)

    def test_parser(self):
        date_time = '2019-01-01T00:00:00Z'
        datetime_obj = date_formatter.parse(date_time)
        formatted = date_formatter.convert(datetime_obj)
        self.assertEqual(formatted, date_time)

    def test_dates_between(self):
        start_date = date(2019, 2, 24)
        end_date = date(2019, 3, 3)
        dates = []
        for returned_date in date_formatter.dates_between(start_date, end_date):
            dates.append(returned_date)
        self.assertEqual(len(dates), 8)

    def test_parse_date(self):
        date_obj = date(2020, 3, 4)
        expected = ('2020', '03', '04')
        result = date_formatter.parse_date(date_obj)
        self.assertEqual(expected, result)

    def test_parse_date_path(self):
        date_input = '2020-01-02T01:02:03Z'
        date_expected = date_formatter.parse(date_input)
        date_path = Path(f'/pfs/tick/{date_input}')
        date_result = date_formatter.parse_date_path(date_path)
        self.assertEqual(date_expected, date_result)


if __name__ == '__main__':
    unittest.main()
