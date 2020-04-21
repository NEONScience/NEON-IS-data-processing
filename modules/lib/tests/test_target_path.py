#!/usr/bin/env python3
import os

import unittest

import lib.target_path as target_path


class TestTargetPath(unittest.TestCase):

    def test_target_path(self):
        file_path = os.path.join('source_type', 'year', 'month', 'day', 'source_id', 'data.ext')
        source_path = os.path.join('/', 'pfs', 'repo_name', file_path)
        out_dir = os.path.join('/', 'pfs', 'out')
        target = target_path.get_path(source_path, out_dir)
        expected_target = os.path.join(out_dir, file_path)
        self.assertTrue(target == expected_target)


if __name__ == '__main__':
    unittest.main()
