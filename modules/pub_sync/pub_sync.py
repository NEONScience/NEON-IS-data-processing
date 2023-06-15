#!/usr/bin/env python3

from pathlib import Path
import environs
import structlog
from contextlib import closing
import os
import datetime
from pathlib import Path
from typing import List

from dateutil.relativedelta import relativedelta
import common.log_config as log_config
from common.get_path_key import get_path_key
from data_access.db_config_reader import read_from_mount
from data_access.db_connector import DbConnector
from data_access.get_dp_pub_records import get_dp_pub_records
from data_access.remove_pub import remove_pub

log = structlog.get_logger()


def pub_sync(connector: DbConnector,
             data_path: Path,
             date_path_year_index: str,
             date_path_month_index: str,
             data_path_product_index: str,
             data_path_site_index: str,
             data_path_date_index: str,
             data_path_package_index: str,
             dp_ids: List[str],
             sites: List[str],
             change_by: str) -> None:

# Get pub months to evaluate
    pub_dates = {}
    date_path_indices = (date_path_year_index,date_path_month_index)
    date_path_min_index = min(date_path_indices)
    date_path_max_index = max(date_path_indices)
    date_path_start = Path(*date_path.parts[0:date_path_min_index])  # Parent of the min index
    for path in date_path_start.rglob('*'):
        if len(path.parts) - 1 == date_path_max_index:
            date_key = get_path_key(path,date_path_indices)  # YYYMM
            year = int(date_key[0:4])
            month = int(date_key[4:6])
            data_interval_start = datetime.date(year,month,1)
            next_month = data_interval_start + relativedelta(days=+32)
            data_interval_end = datetime.date(next_month.year,next_month.month,1)
            cutoff_date = data_interval_end.strftime('%Y%m')
            pub_dates[date_key] = [date_key + '01T00:00:00Z',
                               cutoff_date + '01T00:00:00Z']  # Start date and cutoff date for monthly pub

    log.info(f'Publication months to be evaluated: {",".join(pub_dates.keys())}')

# What product-site-month-packages are output by current processing?
    psmp_pachy = {}  # Dictionary list of product-site-month-packages.
    if data_path is not None:
        data_path_indices = (data_path_product_index,data_path_site_index,data_path_date_index,data_path_package_index)
        data_path_min_index = min(data_path_indices)
        data_path_max_index = max(data_path_indices)
        data_path_start = Path(*data_path.parts[0:data_path_min_index])  # Parent of the min index
        for path in data_path_start.rglob('*'):
            if len(path.parts) - 1 == data_path_max_index:
                log.debug(f'Found output publication package at {path}')

            # Add to the dictionary list with the package
                pub_key = get_path_key(path,data_path_indices)
                psmp_pachy[pub_key] = [path.parts[index] for index in data_path_indices]

    log.info(f'Found {len(psmp_pachy.keys())} product-site-month-packages output by current processing')

# Check existing pubs for relevant site-months against what current output. Generate a list of existing pub records
# that should be inactive (i.e. not currently output). Delete/insert inactive pub records
# as appropriate, to remove visibility
    with closing(DbConnector(db_config)) as connector:
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
