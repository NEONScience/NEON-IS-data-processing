#!/usr/bin/env python3
from contextlib import closing
from typing import Iterator
import common.date_formatter as date_formatter

from typing import NamedTuple,List,Optional
from datetime import datetime

from data_access.db_connector import DbConnector
from data_access.types.dp_pub import DpPub


def get_dp_pub_records(connector: DbConnector,dp_id: str,data_begin: datetime,data_cutoff: datetime,
                       site: None) -> Iterator[DpPub]:
    """
    Get dp pub records for a dp_id, dataIntervalStart, dataIntervalEnd and any site.

    :param connector: A database connector.
    :param dp_id, dataIntervalStart, dataIntervalEnd and any site.
    :return: data product records for the . An example query below, 

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
             dp_idq  = %s
         and 
             data_interval_start >= %s
         and 
             data_interval_start < %s
         and                                    /* this clause will be removed
             site = %s                          /* when site is not passed in.
     '''

    connection = connector.get_connection()
    with closing(connection.cursor()) as cursor:
        if site is None:
            cursor.execute(sql.replace("and \n             site = %s",""),(dp_id,data_begin,data_cutoff))
        else:
            cursor.execute(sql,(dp_id,data_begin,data_cutoff,site))
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
            dppub = DpPub(dataProductI=dataProductId,
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
