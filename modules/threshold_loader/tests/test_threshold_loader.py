#!/usr/bin/env python3
import os
import json
from pathlib import Path
import unittest
from typing import Iterator

from pyfakefs.fake_filesystem_unittest import TestCase

from data_access.types.threshold import Threshold
import threshold_loader.threshold_loader_main as threshold_loader_main
from threshold_loader.threshold_loader import load_thresholds


class ThresholdLoaderTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = Path('/out')
        self.fs.create_dir(self.out_path)

    def test_write_file(self):

        def get_thresholds() -> Iterator[Threshold]:
            """
            Mock function for getting thresholds.

            :return: A threshold.
            """
            yield Threshold(threshold_name='threshold_name',
                            term_name='term_name',
                            location_name='CPER',
                            context=['context1', 'context2'],
                            start_date='start_date',
                            end_date='end_date',
                            is_date_constrained=True,
                            start_day_of_year=1,
                            end_day_of_year=365,
                            number_value=10,
                            string_value='value')

        load_thresholds(get_thresholds, self.out_path)
        expected_path = self.out_path.joinpath('thresholds.json')
        self.assertTrue(expected_path.exists())
        with open(expected_path, 'r') as threshold_file:
            json_data = json.load(threshold_file)
            threshold = json_data['thresholds'][0]
            threshold_name: str = threshold['threshold_name']
            term_name: str = threshold['term_name']
            location_name: str = threshold['location_name']
            context: list = threshold['context']
            start_date: str = threshold['start_date']
            end_date: str = threshold['end_date']
            is_date_constrained: bool = threshold['is_date_constrained']
            start_day_of_year = threshold['start_day_of_year']
            end_day_of_year = threshold['end_day_of_year']
            number_value = threshold['number_value']
            string_value = threshold['string_value']
            self.assertTrue(threshold_name == 'threshold_name')
            self.assertTrue(term_name == 'term_name')
            self.assertTrue(location_name == 'CPER')
            self.assertTrue(context[0] == 'context1')
            self.assertTrue(context[1] == 'context2')
            self.assertTrue(start_date == 'start_date')
            self.assertTrue(end_date == 'end_date')
            self.assertTrue(is_date_constrained)
            self.assertTrue(start_day_of_year == 1)
            self.assertTrue(end_day_of_year == 365)
            self.assertTrue(number_value == 10)
            self.assertTrue(string_value == 'value')
            print(json.dumps(json_data, indent=2, sort_keys=False))

    @unittest.skip('Integration test skipped due to long process time.')
    def test_main(self):
        # database URL in the form: postgresql://[user]@[url]:[port]/[database_name]?password=[pass]
        database_url = os.getenv('PG_DATABASE_URL')
        os.environ['DATABASE_URL'] = database_url
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        threshold_loader_main.main()
        expected_path = Path(self.out_path, 'thresholds.json')
        self.assertTrue(expected_path.exists())
