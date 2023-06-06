#!/usr/bin/env python3
import unittest

from data_access.db_config_reader import read_from_environment
from data_access.db_connector import DbConnector
from data_access.tests.database_test import DatabaseBackedTest
from pub_files.database.term_variables import make_get_term_variables, TermVariables


class TermVariablesTest(DatabaseBackedTest):

    @unittest.skip('Integration test skipped.')
    def test_get(self):
        self.configure_mount()
        db_config = read_from_environment()
        connector = DbConnector(db_config)
        get_term_variables = make_get_term_variables(connector)
        data_product_id = 'NEON.DOM.SITE.DP1.00098.001.00672.HOR.VER.030'
        term_name = 'RHPersistenceFailQM'
        variables: TermVariables = get_term_variables(data_product_id, term_name)
        assert variables.data_type == 'real'
