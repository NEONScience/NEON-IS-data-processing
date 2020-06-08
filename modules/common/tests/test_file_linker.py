#!/usr/bin/env python3
import unittest
from pathlib import Path

from pyfakefs import fake_filesystem_unittest
from common import file_linker


class FileLinkerTest(fake_filesystem_unittest.TestCase):

    def setUp(self):
        self.setUpPyfakefs()

    def test_link(self):
        file_path = Path('/test/input/file.foo')
        link_path = Path('/test/output/file.foo')
        self.fs.create_file(file_path)
        self.assertTrue(file_path.exists())
        self.assertFalse(link_path.exists())
        file_linker.link(file_path, link_path)
        self.assertTrue(link_path.exists())


if __name__ == '__main__':
    unittest.main()
