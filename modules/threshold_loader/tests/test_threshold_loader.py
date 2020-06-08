#!/usr/bin/env python3
import os
import json
from pathlib import Path
import unittest

from pyfakefs.fake_filesystem_unittest import TestCase

import threshold_loader.app as app


class ThresholdLoaderTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = Path('/output')
        self.fs.create_dir(self.out_path)
        #  database URL in the form: [user]/[pass]@[url]:[port]/[sid]
        self.database_url = os.getenv('DATABASE_URL')

    def test_write_threshold_file(self):
        thresholds = []
        threshold = {}
        threshold.update({'threshold_name': 'threshold_name'})
        threshold.update({'term_name': 'term_name'})
        threshold.update({'location_name': 'location_name'})
        threshold.update({'context': ['context1', 'context2']})
        threshold.update({'start_date': 'start_date'})
        threshold.update({'end_date': 'end_date'})
        threshold.update({'is_date_constrained': 'is_date_constrained'})
        threshold.update({'start_day_of_year': 'start_day_of_year'})
        threshold.update({'end_day_of_year': 'end_day_of_year'})
        threshold.update({'number_value': 'number_value'})
        threshold.update({'string_value': 'string_value'})
        thresholds.append(threshold)
        expected_path = self.out_path.joinpath('thresholds.json')
        app.write_threshold_file(thresholds, self.out_path)
        self.assertTrue(expected_path.exists())
        with open(expected_path, 'r') as threshold_file:
            json_data = json.load(threshold_file)
            threshold = json_data['thresholds'][0]
            threshold_name = threshold['threshold_name']
            self.assertTrue(threshold_name == 'threshold_name')
            print(json.dumps(json_data, indent=2, sort_keys=False))

    @unittest.skip('Skip due to long process time.')
    def test_main(self):
        os.environ['DATABASE_URL'] = self.database_url
        os.environ['OUT_PATH'] = self.out_path
        os.environ['LOG_LEVEL'] = 'DEBUG'
        app.main()
        expected_path = self.out_path.joinpath('thresholds.json')
        self.assertTrue(expected_path.exists())


if __name__ == '__main__':
    unittest.main()
