import unittest
import logging

import lib.log_config as log_config


class TestLogConfig(unittest.TestCase):

    def test_get_level(self):
        self.assertEqual(log_config.get_level('DEBUG'), logging.DEBUG)


if __name__ == '__main__':
    unittest.main()
