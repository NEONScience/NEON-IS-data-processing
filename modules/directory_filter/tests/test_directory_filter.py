#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

import directory_filter.app as app
from directory_filter.filter import filter_directory
import lib.log_config as log_config


class DirectoryFilterTest(TestCase):

    def setUp(self):
        log_config.configure('DEBUG')

        self.setUpPyfakefs()

        self.in_dir = Path('/', 'inputs')
        self.out_path = Path('/', 'outputs')

        self.dir_1 = 'dir1'
        self.dir_2 = 'dir2'
        self.dir_3 = 'dir3'

        self.file_name1 = 'dir1.ext'
        self.file_name2 = 'dir2.ext'
        self.file_name3 = 'dir3.ext'

        self.in_path_dir = Path(self.in_dir, 'dir')

        file1 = self.in_path_dir.joinpath(self.dir_1, self.file_name1)
        file2 = self.in_path_dir.joinpath(self.dir_2, self.file_name2)
        file3 = self.in_path_dir.joinpath(self.dir_3, self.file_name3)
        self.fs.create_file(file1)
        self.fs.create_file(file2)
        self.fs.create_file(file3)

        self.out_path1 = self.out_path.joinpath(self.dir_1, self.file_name1)
        self.out_path2 = self.out_path.joinpath(self.dir_2, self.file_name2)
        self.out_path3 = self.out_path.joinpath(self.dir_3, self.file_name3)

        # the directories to pass the filter
        self.filter_dirs = self.dir_1 + ',' + self.dir_2
        # trim root and the first directory '/inputs' from the input paths
        self.relative_path_index = 3

    def test_filter_single_dir(self):
        dir1 = self.parse_dirs(self.dir_1)
        filter_directory(self.in_dir, self.out_path, dir1, self.relative_path_index)
        self.assertTrue(self.out_path1.exists())
        self.assertFalse(self.out_path2.exists())
        self.assertFalse(self.out_path3.exists())

    def test_filter_two_dirs(self):
        dirs = self.parse_dirs(self.filter_dirs)
        filter_directory(self.in_dir, self.out_path, dirs, self.relative_path_index)
        self.assertTrue(self.out_path1.exists())
        self.assertTrue(self.out_path2.exists())
        self.assertFalse(self.out_path3.exists())

    def test_main(self):
        os.environ['IN_PATH'] = str(self.in_dir)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['FILTER_DIR'] = self.filter_dirs
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['RELATIVE_PATH_INDEX'] = str(self.relative_path_index)
        app.main()
        self.assertTrue(self.out_path1.exists())
        self.assertTrue(self.out_path2.exists())
        self.assertFalse(self.out_path3.exists())

    @staticmethod
    def parse_dirs(filter_dirs):
        """
        Parse the comma separated string into a list of directory names.

        :param filter_dirs: A comma separated string of the directories to filter.
        :type filter_dirs: str
        :return: A list of directory names to pass through the filter.
        """
        dirs = []
        if ',' in filter_dirs:
            dirs = filter_dirs.split(',')
            return dirs
        else:
            dirs.append(filter_dirs)
            return dirs
