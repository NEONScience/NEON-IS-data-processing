#!/usr/bin/env python3
from contextlib import closing
from typing import Iterator
import common.date_formatter as date_formatter

from data_access.db_connector import DbConnector
from data_access.types.dp_pub import DpPub


def get_srf_loaders(connector: DbConnector, , location_type: str, source_type: str) -> Iterator[DpPub]:
    """
    Get science_review data for a group prefix, i.e., pressure-air_.

    :param connector: A database connector.
    :param group_prefix: A group prefix, i.e., rel-humidity_ or surfacewater-physical_.
    :return: The Srf.
    """

    sql = f'''
         select 
             dp_idq as dataProductId, 
             site, 
             data_interval_start as dataIntervalStart,
             data_interval_end as dataIntervalEnd, 
             package_type as packageType, 
             has_data as hasData, 
             status, 
             create_date, 
             update_date, 
             release_status as releaseStatus, 
             dp_pub_id as id
         from 
             dp_pub 
         where 
             dp_idq  = %s
         and 
             data_interval_start >= %s
         and 
             data_interval_start < %s
         and 
             site = %s
     '''
