import os

import unittest
from pyfakefs.fake_filesystem_unittest import TestCase

from lib import log_config as log_config
import file_joiner.app as app


class AppTest(TestCase):

    def setUp(self):
        """Create files to join in fake filesystem."""
        log_config.configure('DEBUG')
        self.setUpPyfakefs()

        self.input_path = os.path.join('/', 'repo', 'inputs')
        self.output_path = os.path.join('/', 'outputs')

        self.path_1 = os.path.join('dir1', 'dir2', 'file_1.txt')
        self.path_2 = os.path.join('dir1', 'dir2', 'file_2.txt')
        self.path_3 = os.path.join('dir1', 'dir3', 'file_3.txt')

        self.input_path_1 = os.path.join(self.input_path, self.path_1)
        self.input_path_2 = os.path.join(self.input_path, self.path_2)
        self.input_path_3 = os.path.join(self.input_path, self.path_3)

        self.fs.create_file(self.input_path_1)
        self.fs.create_file(self.input_path_2)
        self.fs.create_file(self.input_path_3)

        self.pathname = str(os.path.join('/', '*', '*', 'dir1', 'dir2', '**'))

    def test_join(self):
        app.join(self.pathname, self.output_path)
        self.check_output()

    def test_main(self):
        os.environ['PATHNAME'] = self.pathname
        os.environ['OUT_PATH'] = self.output_path
        os.environ['LOG_LEVEL'] = 'DEBUG'
        app.main()
        self.check_output()

    def check_output(self):
        path_1 = os.path.join(self.output_path, self.path_1)
        path_2 = os.path.join(self.output_path, self.path_2)
        path_3 = os.path.join(self.output_path, self.path_3)
        self.assertTrue(os.path.lexists(path_1))
        self.assertTrue(os.path.lexists(path_2))
        self.assertFalse(os.path.lexists(path_3))


if __name__ == '__main__':
    unittest.main()
