#!/usr/bin/env python3
from contextlib import closing
from datetime import datetime
from typing import Iterator

import common.date_formatter as date_formatter
from data_access.db_connector import DbConnector
from data_access.types.dp_pub import DpPub


def get_dp_pub_records(connector: DbConnector, dp_id: str, data_begin: datetime, data_cutoff: datetime,
                       site: None) -> Iterator[DpPub]:
    """
    Get dp pub records for a dp_id, dataIntervalStart, dataIntervalEnd and any site.

    :param connector: A database connector.
    :param dp_id: The data product ID.
    :param data_begin: The start time for data.
    :param data_cutoff: The end time for data.
    :param site: The site.
    :return: data product records for the select criteria. An example query below, 

    select * from dp_pub 
    where dp_idq = 'NEON.DOM.SITE.DP1.00017.001'
    and data_interval_start >= '2013-10-01T00:00:00Z' 
    and data_interval_start < '2013-11-01T00:00:00Z'
    (and site = 'STER')                                 /* site can be None
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
             dp_idq = %s
         and 
             data_interval_start >= %s
         and 
             data_interval_start < %s
         and 
             site = %s
     '''

    connection = connector.get_connection()
    with closing(connection.cursor()) as cursor:
        if site is None:
            sql2 = sql.replace("and \n             site = %s", "")
            cursor.execute(sql2, (dp_id, data_begin, data_cutoff))
        else:
            cursor.execute(sql, (dp_id, data_begin, data_cutoff, site))
        rows = cursor.fetchall()
        for row in rows:
            data_product_id = row[0]
            site = row[1]
            data_interval_start = row[2]
            data_interval_end = row[3]
            package_type = row[4]
            has_data = row[5]
            status = row[6]
            create_date = row[7]
            update_date = row[8]
            release_status = row[9]
            row_id = row[10]
            dp_pub = DpPub(dataProductId=data_product_id,
                           site=site,
                           dataIntervalStart=date_formatter.to_string(data_interval_start),
                           dataIntervalEnd=date_formatter.to_string(data_interval_end),
                           packageType=package_type,
                           hasData=has_data,
                           status=status,
                           create_date=date_formatter.to_string(create_date),
                           updateDate=date_formatter.to_string(update_date),
                           releaseStatus=release_status,
                           id=row_id)
            yield dp_pub
