#!/usr/bin/env python3
import os
import unittest
from unittest.mock import patch

from os_table_loader.data.lov_values_loader import get_lov_values


class FakeResponse:

    def __init__(self, payload: str):
        self._payload = payload

    def read(self):
        return self._payload.encode('utf-8')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class LovValuesLoaderTest(unittest.TestCase):

    @patch('os_table_loader.data.lov_values_loader.urlopen')
    def test_get_lov_values_returns_code_and_description(self, mock_urlopen):
        os.environ['LOV_BASE_URL'] = 'https://example.org/os-api'
        mock_urlopen.return_value = FakeResponse('{"items":[{"itemCode":"Y","itemDescription":"Yes"}]}')

        values = get_lov_values('Yes or No choice')

        self.assertEqual(values, [{'code': 'Y', 'description': 'Yes'}])
        called_url = mock_urlopen.call_args.args[0]
        self.assertTrue(called_url.endswith('/list-of-values/Yes%20or%20No%20choice'))


if __name__ == '__main__':
    unittest.main()
