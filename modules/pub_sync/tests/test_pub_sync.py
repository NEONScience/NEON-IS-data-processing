#!/usr/bin/env python3

import unittest
from pathlib import Path
from unittest import TestCase

from typing import Callable,Iterator,List,Dict
from data_access.types.dp_pub import DpPub

from pub_sync.tests.dp_pub_data import get_dp_pub_data
from testfixtures import TempDirectory

from pub_sync import pub_sync_main,pub_sync


class PubSyncTest(TestCase):

    def test_pub_sync(self):
        temp_dir = TempDirectory()
        temp_dir_name = temp_dir.path
        date_path_year_index = "3"
        date_path_month_index = "4"
        data_path_product_index = "3"
        data_path_site_index = "4"
        data_path_date_index = "5"
        data_path_package_index = "6"
        dp_ids: List[str] = ["NEON.DOM.SITE.DP1.00066.001"]
        sites: List[str] = ["MOD1","YELL", "CPER"]
        data_path = None
        date_path = Path('2023/04/01')
        date_path_indices = (date_path_year_index,date_path_month_index)
        date_path_min_index = int(min(date_path_indices))
        date_path_max_index = int(max(date_path_indices))
        change_by = 'pachyderm1'

        def get_sync_pubs(pub_dates: List[Dict],dp_ids: List[str],sites: List[str],psmp_pachy: List[Dict],
                          change_by: str) -> List[DpPub]:
            """Mock function"""

            date_key = '202301'
            cutoff_date = '202304'
            pub_dates[date_key] = [date_key + '01T00:00:00Z',
                                   cutoff_date + '01T00:00:00Z']
            pubs: List[DpPub] = []
            pubs = get_dp_pub_data()
            #
            for pub in pubs:
                #   Form the key for matching existing portal pubs to pachy pubs
                dataIntervalStartKey = pub.dataIntervalStart.replace('Z','').replace(':','').replace('-','')
                dataIntervalEndKey = pub.dataIntervalEnd.replace('Z','').replace(':','').replace('-','')
                pub_key = pub.dataProductId + pub.site + dataIntervalStartKey + '--' + dataIntervalEndKey + pub.packageType

        pub_sync.sync_pubs(get_sync_pubs=get_sync_pubs,
                           data_path=data_path,
                           date_path=date_path,
                           date_path_year_index=date_path_year_index,
                           date_path_month_index=date_path_month_index,
                           data_path_product_index=data_path_product_index,
                           data_path_site_index=data_path_site_index,
                           data_path_date_index=data_path_date_index,
                           data_path_package_index=data_path_package_index,
                           dp_ids=dp_ids,
                           sites=sites,
                           change_by=change_by)

        self.assertTrue(change_by == 'pachyderm1')


if __name__ == '__main__':
    unittest.main()
