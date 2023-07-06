#!/usr/bin/env python3
from typing import List,Dict, NamedTuple
import unittest

from pathlib import Path
from unittest import TestCase

from data_access.types.dp_pub import DpPub
from typing import Callable, Iterator, List, Dict

from dateutil.relativedelta import relativedelta
from common.get_path_key import get_path_key

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
        sites: List[str] = ["CPER","HARV","ABBY"]
        data_path = None
        date_path = Path('2023/04/01')
        date_path_indices = (date_path_year_index,date_path_month_index)
        date_path_min_index = int(min(date_path_indices))
        date_path_max_index = int(max(date_path_indices))
        date_path_start = Path(*date_path.parts[0:date_path_min_index])  # Parent of the min index
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
            psmp_portal_remove = {}
            #
            for pub in pubs:
             #   Form the key for matching existing portal pubs to pachy pubs
                dataIntervalStartKey = pub.dataIntervalStart.replace('Z','').replace(':','').replace('-','')
                dataIntervalEndKey = pub.dataIntervalEnd.replace('Z','').replace(':','').replace('-','')
                pub_key = pub.dataProductId + pub.site + dataIntervalStartKey + '--' + dataIntervalEndKey + pub.packageType

            #    If an existing portal pub is not the list of current pubs, mark it for further investigation & possible removal
                if pub_key not in psmp_pachy.keys():
                    if pub_key in psmp_portal_remove.keys():
                        pub_list = psmp_portal_remove[pub_key]
                        pub_list.append(pub)
                        psmp_portal_remove[pub_key] = pub_list
                    else:
                        psmp_portal_remove[pub_key] = [pub]
                        # log.debug(
                        #     f'Found pub records for package [{pub.dataProductId} {pub.site} {pub.dataIntervalStart} {pub.packageType}] not output by current processing. Marked for investigation.')

        # Check or set the relevant portal records to inactive
        #   log.info(f'Found {len(psmp_portal_remove.keys())} product-site-month-packages to check/set to inactive')
        #    remove_pub(connector,psmp_portal_remove,change_by)
            pub_dates = {}
            date_path_indices = (date_path_year_index,date_path_month_index)
            date_path_min_index = int(min(date_path_indices))
            date_path_max_index = int(max(date_path_indices))
            date_path_start = Path(*date_path.parts[0:date_path_min_index])  # Parent of the min index

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
