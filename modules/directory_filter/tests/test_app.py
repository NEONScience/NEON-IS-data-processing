import os

from pyfakefs.fake_filesystem_unittest import TestCase

import directory_filter.app as app
import lib.log_config as log_config


class BaseTest(TestCase):

    def setUp(self):

        log_config.configure('DEBUG')

        self.setUpPyfakefs()

        self.out_path = os.path.join('/', 'outputs')

        self.reject_path = 'dir1'  # The directory to be rejected by filtering.
        self.accept_path = 'dir2'  # The directory to pass filtering.

        self.file_name1 = 'dir1.avro'
        self.file_name2 = 'dir2.avro'

        #  Set required files in mock filesystem.
        self.in_dir = os.path.join('/', 'inputs')
        in_dir_path = os.path.join(self.in_dir, 'dir')

        reject = os.path.join(in_dir_path, self.reject_path, self.file_name1)
        accept = os.path.join(in_dir_path, self.accept_path, self.file_name2)
        self.fs.create_file(reject)
        self.fs.create_file(accept)

    def test_filter(self):
        app.filter_directory(self.in_dir, self.accept_path, self.out_path)
        self.check_output()

    def test_main(self):
        os.environ['IN_PATH'] = self.in_dir
        os.environ['OUT_PATH'] = self.out_path
        os.environ['FILTER_DIR'] = 'dir2'
        os.environ['LOG_LEVEL'] = 'DEBUG'
        app.main()
        self.check_output()

    def check_output(self):
        #  Only sub_dir should have passed the filter.
        result_dir = os.path.join(self.out_path, self.accept_path)
        self.assertTrue(os.path.lexists(result_dir))
        self.assertTrue(os.path.exists(os.path.join(result_dir, self.file_name2)))
        self.assertFalse(os.path.exists(os.path.join(self.out_path, self.reject_path)))
