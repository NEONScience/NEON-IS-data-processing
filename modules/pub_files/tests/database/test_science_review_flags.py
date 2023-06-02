#!/usr/bin/env python3
import unittest
from typing import List, Callable

from data_access.db_config_reader import read_from_environment
from data_access.db_connector import DbConnector
from data_access.tests.database_test import DatabaseBackedTest
from pub_files.database.science_review_flags import make_get_flags, ScienceReviewFlag


class ScienceReviewFlagTest(DatabaseBackedTest):

    @unittest.skip('Integration test skipped.')
    def test_get_data_product(self):
        self.configure_mount()
        db_config = read_from_environment()
        connector = DbConnector(db_config)
        data_product_id = 'DP1.00023.001'
        site = 'OAES'
        get_flags: Callable[[str, str], List[ScienceReviewFlag]] = make_get_flags(connector)
        flags = get_flags('DP1.00066.001', 'CPER')
        # assert len(flags) == 18
        print(f'flags: {flags}')
