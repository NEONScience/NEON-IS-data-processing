#!/usr/bin/env python3

from pathlib import Path
import environs
import structlog
from contextlib import closing
import os
import datetime
from pathlib import Path
from typing import List, Dict

from dateutil.relativedelta import relativedelta
import common.log_config as log_config
from common.get_path_key import get_path_key
from data_access.db_config_reader import read_from_mount
from data_access.db_connector import DbConnector
from data_access.get_dp_pub_records import get_dp_pub_records
from data_access.remove_pub import remove_pub
from data_access.types.dp_pub import DpPub

log = structlog.get_logger()

def get_sync_pubs(connector: DbConnector, pub_dates: List[Dict], dp_ids: List[str], sites: List[str], psmp_pachy: List[Dict] ) -> None:

# Check existing pubs for relevant site-months against what current output. Generate a list of existing pub records
# that should be inactive (i.e. not currently output). Delete/insert inactive pub records
# as appropriate, to remove visibility

#    with closing(DbConnector(db_config)) as connector:

    connection = connector.get_connection()
    with closing(connection.cursor()) as cursor:
        psmp_portal_remove = {}

        for date_key in list(pub_dates.keys()):
            data_begin = pub_dates[date_key][0]
            data_cutoff = pub_dates[date_key][1]

            for dp_id in dp_ids:
                for site in sites:
                    if site == 'all':
                        site = None
                    pubs = get_dp_pub_records(connector,dp_id,data_begin,data_cutoff,site)

                # Check existing product-site-month pubs against pachy pubs for product
                    for pub in pubs:

                        # Form the key for matching existing portal pubs to pachy pubs
                        dataIntervalStartKey = pub.dataIntervalStart.replace('Z','').replace(':','').replace('-','')
                        dataIntervalEndKey = pub.dataIntervalEnd.replace('Z','').replace(':','').replace('-','')
                        pub_key = pub.dataProductId + pub.site + dataIntervalStartKey + '--' + dataIntervalEndKey + pub.packageType

                        # If an existing portal pub is not the list of current pubs, mark it for further investigation & possible removal
                        if pub_key not in psmp_pachy.keys():
                            if pub_key in psmp_portal_remove.keys():
                                pub_list = psmp_portal_remove[pub_key]
                                pub_list.append(pub)
                                psmp_portal_remove[pub_key] = pub_list
                            else:
                                psmp_portal_remove[pub_key] = [pub]
                                log.debug(
                                    f'Found pub records for package [{pub.dataProductId} {pub.site} {pub.dataIntervalStart} {pub.packageType}] not output by current processing. Marked for investigation.')

        # Check or set the relevant portal records to inactive
        log.info(f'Found {len(psmp_portal_remove.keys())} product-site-month-packages to check/set to inactive')
        remove_pub(connector,psmp_portal_remove,change_by)