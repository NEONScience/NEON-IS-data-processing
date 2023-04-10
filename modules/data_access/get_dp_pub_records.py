#!/usr/bin/env python3
from contextlib import closing
from typing import Iterator
import common.date_formatter as date_formatter

from typing import NamedTuple,List,Optional
from datetime import datetime

from data_access.db_connector import DbConnector
from data_access.types.dp_pub import DpPub


def get_dp_pub_records(connector: DbConnector,dp_ids: List[str],data_begin: datetime,data_cutoff: datetime,
                       sites: List[str]) -> Iterator[DpPub]:
    """
    Get dp pub records for a dp_id(s), dataIntervalStart, dataIntervalEnd and any site(s).

    :param connector: A database connector.
    :param dp_id, dataIntervalStart, dataIntervalEnd and any site: A data product id.
    :return: data product records.

    select * from dp_pub
    where site in ('STER', 'ABBY')
    and dp_idq  in ('NEON.DOM.SITE.DP1.00017.001', 'NEON.DOM.SITE.DP1.00040.001')
    and data_interval_start >= '2013-10-01T00:00:00Z'
    and data_interval_end <= '2013-11-01T00:00:00Z';
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
             dp_idq  = ANY (%s)
         and 
             data_interval_start >= %s
         and 
             data_interval_start < %s
         and 
             site = ANY (%s)
     '''

    connection = connector.get_connection()
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql,(dp_ids,data_begin,data_cutoff,sites))
        rows = cursor.fetchall()
        for row in rows:
            dataProductId = row[0]
            site = row[1]
            dataIntervalStart = row[2]
            dataIntervalEnd = row[3]
            packageType = row[4]
            hasData = row[5]
            status = row[6]
            create_date = row[7]
            updateDate = row[8]
            releaseStatus = row[9]
            id = row[10]
            dppub = DpPub(dataProductI=ddataProductId,
                          site=site,
                          dataIntervalStart=date_formatter.to_string(dataIntervalStart),
                          dataIntervalEnd=date_formatter.to_string(dataIntervalEnd),
                          packageType=packageType,
                          hasData=hasData,
                          status=status,
                          create_date=date_formatter.to_string(create_date),
                          updateDate=date_formatter.to_string(updateDate),
                          releaseStatus=releaseStatus,
                          id=id)
            yield dppub
