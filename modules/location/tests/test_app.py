#!/usr/bin/env python3
import os

import unittest

from pyfakefs.fake_filesystem_unittest import TestCase

import location.app as app


class AppTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = os.path.join('/', 'output')
        self.fs.create_dir(self.out_path)
        #  Database URL in the form: [user]/[pass]@[url]:[port]/[sid]
        self.database_url = os.getenv('DATABASE_URL')

    @unittest.skip('Skip due to long process time.')
    def test_app(self):
        os.environ['DATABASE_URL'] = self.database_url
        os.environ['OUT_PATH'] = self.out_path
        os.environ['LOG_LEVEL'] = 'DEBUG'
        app.main()
        self.check_output()

    def check_output(self):
        location_file_path = os.path.join(self.out_path, 'CFGLOC100140.json')
        self.assertTrue(os.path.exists(location_file_path))


if __name__ == '__main__':
    unittest.main()
