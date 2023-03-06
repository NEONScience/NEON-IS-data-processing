#!/usr/bin/env python3
import unittest

from data_access.db_config_reader import read_from_environment
from data_access.db_connector import DbConnector
from data_access.tests.database_test import DatabaseBackedTest
from readme_generator.database_queries.data_product import get_data_product


class DataProductTest(DatabaseBackedTest):

    @unittest.skip('Integration test skipped.')
    def test_get_data_product(self):
        self.configure_mount()
        dp_config = read_from_environment()
        idq = 'NEON.DOM.SITE.DP1.00001.001'
        data_product = get_data_product(DbConnector(dp_config), idq)
        assert data_product.idq == idq
        assert data_product.short_idq == 'DP1.00001.001'
        assert data_product.name == '2D wind speed and direction'
        assert data_product.type_name == 'TIS Data Product Type'
        assert data_product.supplier == 'TIS'
        assert data_product.short_name == 'wind-2d'
        assert data_product.sensor == 'Gill - Wind Observer II; Extreme Weather Wind Observer'
