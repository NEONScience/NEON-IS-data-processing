#!/usr/bin/env python3
from contextlib import closing
from typing import List, Iterator
import common.date_formatter as date_formatter

from data_access.db_connector import DbConnector
from data_access.types.srf import Srf


def get_srf_loaders(connector: DbConnector, group_prefix: str) -> Iterator[Srf]:
    """
    Get science_review data for a group prefix, i.e., pressure-air_.

    :param connector: A database connector.
    :param group_prefix: A group prefix, i.e., rel-humidity_ or surfacewater-physical_.
    :return: The Srf.
    """

    sql = f'''
        select 
            g.group_name, sr.id, sr.start_date, sr.end_date, sr.meas_strm_name, 
            t.term_name as "srfTermName", sr.srf, sr.user_comment, sr.create_date, sr.last_update
        from 
            "group" g , data_product_group dpg, science_review sr, term t
        where 
            g.group_name like %s
        and 
            g.group_id = dpg.group_id 
        and 
            substring (dpg.dp_idq  from 15 for 13 ) = substring (sr.meas_strm_name from 15 for 13)
        and 
            t.term_number = cast(substring (sr.meas_strm_name from 29 for 5)  AS INTEGER) 
             
     '''

    group_prefix_1 = group_prefix + '%'
    if group_prefix[-1] == "_":
      group_prefix_1 = group_prefix[:-1] + '\_%'
    connection = connector.get_connection()
    with closing(connection.cursor()) as cursor:
      cursor.execute(sql, [group_prefix_1])
      rows = cursor.fetchall()
      for row in rows:
        group_name = row[0]
        sr_id = row[1]
        start_date = row[2]
        end_date = row[3]
        meas_strm_name = row[4]
        srfTermName = row[5]
        srf = row[6]
        user_comment = row[7]
        create_date = row[8]
        last_update = row[9]
        srf = Srf(group_name=group_name,
                      id=sr_id,
                      start_date=date_formatter.to_string(start_date),
                      end_date=date_formatter.to_string(end_date),
                      measurement_stream_name=meas_strm_name,
                      srf_term_name=srfTermName,
                      srf=srf,
                      user_comment=user_comment,
                      create_date=date_formatter.to_string(create_date),
                      last_update_date=date_formatter.to_string(last_update))
        yield srf
