#!/usr/bin/env python3
from contextlib import closing
from typing import Iterator
import common.date_formatter as date_formatter

from typing import NamedTuple,List,Optional
from datetime import datetime

from data_access.db_connector import DbConnector
from data_access.types.dp_pub import DpPub


def get_dp_pub_records(connector: DbConnector,dp_id: List[str],data_begin: datetime,data_cutoff: datetime,
                    site: List[str]) -> Iterator[DpPub]:
    """
    Get dp pub records for a dp_id, dataIntervalStart, dataIntervalEnd and any site.

    :param connector: A database connector.
    :param dp_id, dataIntervalStart, dataIntervalEnd and any site: A data product id.
    :return: data product records.
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
