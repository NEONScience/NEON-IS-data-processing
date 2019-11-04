import os
from datetime import date

import unittest

from pyfakefs.fake_filesystem_unittest import TestCase

import location_active_dates.app as app
import lib.log_config as log_config

log = log_config.configure('DEBUG')


class AppTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = os.path.join('/', 'output')
        #  Set output directory in mock filesystem.
        self.fs.create_dir(self.out_path)
        #  Database URL in the form: [user]/[pass]@[url]:[port]/[sid]
        self.database_url = os.getenv('DATABASE_URL')

    @unittest.skip('Skip due to long process time.')
    def test_app(self):
        os.environ['CONTEXT'] = 'soil'
        os.environ['LOCATION_TYPE'] = 'CONFIG'
        os.environ['DATABASE_URL'] = self.database_url
        os.environ['tick'] = '/pfs/tick/2019-11-01T00:00:00Z'
        os.environ['OUT_PATH'] = self.out_path
        os.environ['LOG_LEVEL'] = 'DEBUG'
        app.main()
        self.check_output()

    def test_date_generator(self):
        start_date = date(2019, 2, 24)
        end_date = date(2019, 3, 3)
        dates = app.dates_between(start_date, end_date)
        self.assertEqual(len(dates), 8)

    def check_output(self):
        file_path = os.path.join(self.out_path, '2019', '10', '20', 'CFGLOC101740', 'CFGLOC101740.json')
        self.assertTrue(os.path.exists(file_path))


if __name__ == '__main__':
    unittest.main()
