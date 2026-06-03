#!/usr/bin/env python3
import os
import unittest
from unittest.mock import Mock, patch

from data_access.db_connector import DbConfig, DbConnector
from os_table_loader.data.lov_values_loader import get_api_host, get_default_lov_base_url, get_lov_values


class LovValuesLoaderTest(unittest.TestCase):

    def test_get_api_host_returns_prefix_before_dash(self):
        self.assertEqual(get_api_host('int-pdr.gcp.neoninternal.org'), 'int')

    @patch.object(DbConnector, '_connect')
    def test_get_default_lov_base_url_uses_connector_host(self, _mock_connect):
        connector = DbConnector(DbConfig(host='int-pdr.gcp.neoninternal.org',
                                         user='user',
                                         password='password',
                                         database_name='database',
                                         schema='schema'))
        self.assertEqual(get_default_lov_base_url(connector),
                         'https://os-api-int.svcs-nonprod.gcp.neoninternal.org/os-api')

    @patch('os_table_loader.data.lov_values_loader.requests.get')
    @patch.object(DbConnector, '_connect')
    def test_get_lov_values_returns_code_and_description(self, _mock_connect, mock_get):
        os.environ['LOV_BASE_URL'] = 'https://example.org/os-api'
        connector = DbConnector(DbConfig(host='int-pdr.gcp.neoninternal.org',
                                         user='user',
                                         password='password',
                                         database_name='database',
                                         schema='schema'))
        response = Mock()
        response.status_code = 200
        response.json.return_value = {
            'listOfValuesItems': [
                {'pubCode': 'Y', 'description': 'Yes', 'effectiveDate': '2012-01-01T00:00:00Z[GMT]'}
            ]
        }
        mock_get.return_value = response

        values = get_lov_values(connector, 'Yes or No choice')

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
