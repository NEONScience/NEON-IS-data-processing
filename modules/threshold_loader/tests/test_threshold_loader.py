#!/usr/bin/env python3
import os
import unittest

from pyfakefs.fake_filesystem_unittest import TestCase

import threshold_loader.app as app


class ThresholdLoaderTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = os.path.join('/', 'output')
        #  Create output directory in mock filesystem.
        self.fs.create_dir(self.out_path)
        #  Database URL in the form: [user]/[pass]@[url]:[port]/[sid]
        self.database_url = os.getenv('DATABASE_URL')

    @unittest.skip('Skip due to long process time.')
    def test_main(self):
        os.environ['DATABASE_URL'] = self.database_url
        os.environ['OUT_PATH'] = self.out_path
        os.environ['LOG_LEVEL'] = 'DEBUG'
        app.main()
        threshold_file = os.path.join(self.out_path, 'thresholds.json')
        self.assertTrue(os.path.exists(threshold_file))


if __name__ == '__main__':
    unittest.main()
