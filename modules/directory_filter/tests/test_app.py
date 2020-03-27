import os

from pyfakefs.fake_filesystem_unittest import TestCase

import directory_filter.app as app
import lib.log_config as log_config


class BaseTest(TestCase):

    def setUp(self):

        log_config.configure('DEBUG')

        self.setUpPyfakefs()

        self.out_path = os.path.join('/', 'outputs')

        self.dir1 = 'dir1'
        self.dir_2 = 'dir2'
        self.dir_3 = 'dir3'

        self.file_name1 = 'dir1.ext'
        self.file_name2 = 'dir2.ext'
        self.file_name3 = 'dir3.ext'

        #  Set required files in mock filesystem.
        self.in_dir = os.path.join('/', 'inputs')
        in_dir_path = os.path.join(self.in_dir, 'dir')

        file1 = os.path.join(in_dir_path, self.dir1, self.file_name1)
        file2 = os.path.join(in_dir_path, self.dir_2, self.file_name2)
        file3 = os.path.join(in_dir_path, self.dir_3, self.file_name3)
        self.fs.create_file(file1)
        self.fs.create_file(file2)
        self.fs.create_file(file3)

        self.out_path1 = os.path.join(self.out_path, self.dir1, self.file_name1)
        self.out_path2 = os.path.join(self.out_path, self.dir_2, self.file_name2)
        self.out_path3 = os.path.join(self.out_path, self.dir_3, self.file_name3)

    def test_filter_dir(self):
        app.filter_directory(self.in_dir, self.dir1, self.out_path)
        self.assertTrue(os.path.lexists(self.out_path1))
        self.assertFalse(os.path.lexists(self.out_path2))
        self.assertFalse(os.path.lexists(self.out_path3))

    def test_filter_dirs(self):
        app.filter_directory(self.in_dir, self.dir1 + ',' + self.dir_2, self.out_path)
        self.assertTrue(os.path.lexists(self.out_path1))
        self.assertTrue(os.path.lexists(self.out_path2))
        self.assertFalse(os.path.lexists(self.out_path3))

    def test_main(self):
        os.environ['IN_PATH'] = self.in_dir
        os.environ['OUT_PATH'] = self.out_path
        os.environ['FILTER_DIR'] = 'dir1,dir2'
        os.environ['LOG_LEVEL'] = 'DEBUG'
        app.main()
        self.assertTrue(os.path.lexists(self.out_path1))
        self.assertTrue(os.path.lexists(self.out_path2))
        self.assertFalse(os.path.lexists(self.out_path3))
