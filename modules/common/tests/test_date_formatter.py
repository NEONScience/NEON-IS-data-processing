#!/usr/bin/env python3
import unittest
from datetime import datetime

import common.date_formatter as date_formatter


class DateFormatterTest(unittest.TestCase):

    def test_to_string(self):
        expected_date_time = '2019-01-01T00:00:00Z'
        date_time = '2019-01-01 00:00:00'
        date_format = '%Y-%m-%d %H:%M:%S'
        converted_date = datetime.strptime(date_time, date_format)
        formatted = date_formatter.to_string(converted_date)
        self.assertEqual(expected_date_time, formatted)

    def test_to_datetime(self):
        date_time = '2019-01-01T00:00:00Z'
        dt = date_formatter.to_datetime(date_time)
        formatted = date_formatter.to_string(dt)
        self.assertEqual(formatted, date_time)
