#!/usr/bin/env python3
import unittest
import os

from pyfakefs import fake_filesystem_unittest
from lib import file_linker


class FileUtilTest(fake_filesystem_unittest.TestCase):

    def setUp(self):
        self.setUpPyfakefs()

    def test_link(self):
        source = '/test/input/file.foo'
        target = '/test/output/file.foo'
        self.fs.create_file(source)
        self.assertTrue(os.path.isfile(source))
        self.assertFalse(os.path.isfile(target))
        file_linker.link(source, target)
        self.assertTrue(os.path.isfile(target))


if __name__ == '__main__':
    unittest.main()
