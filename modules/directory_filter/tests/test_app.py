#!/usr/bin/env python3
import os

from pyfakefs.fake_filesystem_unittest import TestCase

import directory_filter.app as app
from directory_filter.filter import filter_directory, parse_dirs
import lib.log_config as log_config


class BaseTest(TestCase):

    def setUp(self):
        log_config.configure('DEBUG')

        self.setUpPyfakefs()

        self.in_dir = os.path.join('/', 'inputs')
        self.out_path = os.path.join('/', 'outputs')

        self.dir1 = 'dir1'
        self.dir_2 = 'dir2'
        self.dir_3 = 'dir3'

        self.file_name1 = 'dir1.ext'
        self.file_name2 = 'dir2.ext'
        self.file_name3 = 'dir3.ext'

        self.in_path_dir = os.path.join(self.in_dir, 'dir')

        file1 = os.path.join(self.in_path_dir, self.dir1, self.file_name1)
        file2 = os.path.join(self.in_path_dir, self.dir_2, self.file_name2)
        file3 = os.path.join(self.in_path_dir, self.dir_3, self.file_name3)
        self.fs.create_file(file1)
        self.fs.create_file(file2)
        self.fs.create_file(file3)

        self.out_path1 = os.path.join(self.out_path, self.dir1, self.file_name1)
        self.out_path2 = os.path.join(self.out_path, self.dir_2, self.file_name2)
        self.out_path3 = os.path.join(self.out_path, self.dir_3, self.file_name3)

        # the directories to pass the filter
        self.filter_dirs = self.dir1 + ',' + self.dir_2
        # trim root and the first directory '/inputs' from the input paths
        self.relative_path_index = 3

    def test_filter_single_dir(self):
        dir1 = parse_dirs(self.dir1)
        filter_directory(self.in_dir, dir1, self.out_path, self.relative_path_index)
        self.assertTrue(os.path.lexists(self.out_path1))
        self.assertFalse(os.path.lexists(self.out_path2))
        self.assertFalse(os.path.lexists(self.out_path3))

    def test_filter_two_dirs(self):
        dirs = parse_dirs(self.filter_dirs)
        filter_directory(self.in_dir, dirs, self.out_path, self.relative_path_index)
        self.assertTrue(os.path.lexists(self.out_path1))
        self.assertTrue(os.path.lexists(self.out_path2))
        self.assertFalse(os.path.lexists(self.out_path3))

    def test_main(self):
        os.environ['IN_PATH'] = self.in_dir
        os.environ['OUT_PATH'] = self.out_path
        os.environ['FILTER_DIR'] = self.filter_dirs
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['RELATIVE_PATH_INDEX'] = str(self.relative_path_index)
        app.main()
        self.assertTrue(os.path.lexists(self.out_path1))
        self.assertTrue(os.path.lexists(self.out_path2))
        self.assertFalse(os.path.lexists(self.out_path3))
