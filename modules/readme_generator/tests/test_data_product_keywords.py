#!/usr/bin/env python3
import unittest
from typing import List

from data_access.db_config_reader import read_from_environment
from data_access.db_connector import DbConnector
from data_access.tests.database_test import DatabaseBackedTest
from readme_generator.database_queries.data_product_keywords import get_keywords


class DataProductKeywordTest(DatabaseBackedTest):

    @unittest.skip('Integration test skipped.')
    def test_get_keywords(self):
        self.configure_mount()
        dp_config = read_from_environment()
        keywords: List[str] = get_keywords(DbConnector(dp_config), 'NEON.DOM.SITE.DP1.00041.001')
        assert 'soil temperature' in keywords
        assert 'profile' in keywords
        assert 'soil' in keywords
