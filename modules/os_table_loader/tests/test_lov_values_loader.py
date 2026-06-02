#!/usr/bin/env python3
import os
import unittest
from unittest.mock import Mock, patch

from os_table_loader.data.lov_values_loader import get_lov_values


class LovValuesLoaderTest(unittest.TestCase):

    @patch('os_table_loader.data.lov_values_loader.requests.get')
    def test_get_lov_values_returns_code_and_description(self, mock_get):
        os.environ['LOV_BASE_URL'] = 'https://example.org/os-api'
        response = Mock()
        response.status_code = 200
        response.json.return_value = {
            'listOfValuesItems': [
                {'pubCode': 'Y', 'description': 'Yes', 'effectiveDate': '2012-01-01T00:00:00Z[GMT]'}
            ]
        }
        mock_get.return_value = response

        values = get_lov_values('Yes or No choice')

        self.assertEqual(values, [{
            'name': 'Yes or No choice',
            'pubCode': 'Y',
            'description': 'Yes',
            'startDate': '2012-01-01T00:00:00',
            'endDate': ''
        }])
        called_url = mock_get.call_args.args[0]
        self.assertTrue(called_url.endswith('/list-of-values/Yes%20or%20No%20choice'))


if __name__ == '__main__':
    unittest.main()
