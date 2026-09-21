from contextlib import closing
from datetime import datetime
from typing import NamedTuple

from psycopg2.extras import RealDictCursor
from structlog import get_logger

from data_access.db_connector import DbConnector
from os_table_loader.data.table_loader import Table

log = get_logger()


class Result(NamedTuple):
    result_uuid: str
    start_date: datetime
    end_date: datetime
    location_name: str


def get_results(connector: DbConnector, table: Table) -> list[Result]:
    results = []
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'''
        select
            os_result.result_uuid, 
            os_result.start_date, 
            os_result.end_date,
            nam_locn.nam_locn_name 
        from {schema}.os_result
        join {schema}.pub_table_def
          on pub_table_def.pub_table_def_id = os_result.pub_table_def_id
        left join {schema}.nam_locn
          on nam_locn.nam_locn_id = os_result.nam_locn_id
        where
            os_result.pub_table_def_id = %(table_id)s
    '''
    with closing(connection.cursor(cursor_factory=RealDictCursor)) as cursor:
        cursor.execute(sql, dict(table_id=table.id))
        rows = cursor.fetchall()
        for row in rows:
            result_uuid = row['result_uuid']
            start_date = row['start_date']
            end_date = row['end_date']
            location_name = row['nam_locn_name']
            result = Result(result_uuid=result_uuid,
                            start_date=start_date,
                            end_date=end_date,
                            location_name=location_name)
            results.append(result)
    return results


def get_site_results(connector: DbConnector,
                     table: Table,
                     site: str,
                     start_date: datetime,
                     end_date: datetime) -> list[Result]:
    results = []
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'''
        select
            os_result.result_uuid, 
            os_result.start_date, 
            os_result.end_date,
            nam_locn.nam_locn_name 
        from 
            {schema}.os_result, 
            {schema}.pub_table_def, 
            {schema}.nam_locn
        where 
            os_result.pub_table_def_id = %(table_id)s
        and 
            pub_table_def.pub_table_def_id = os_result.pub_table_def_id
        and 
            nam_locn.nam_locn_id = os_result.nam_locn_id
        and
            (nam_locn.nam_locn_name like %(site_pattern)s
             or exists (
                 select 1
                 from {schema}.os_result_data, {schema}.pub_field_def
                 where os_result_data.result_uuid = os_result.result_uuid
                 and os_result_data.pub_field_def_id = pub_field_def.pub_field_def_id
                 and pub_field_def.field_name = 'namedLocation'
                 and os_result_data.string_value like %(site_pattern)s
             ))
        and 
            os_result.start_date >= %(start_date)s
        and 
            os_result.end_date < %(end_date)s
    '''
    query_args = dict(table_id=table.id,
                      site_pattern=f'%{site}%',
                      start_date=start_date,
                      end_date=end_date)
    log.debug('Querying site results',
              table=table.name,
              query_args=query_args)
    with closing(connection.cursor(cursor_factory=RealDictCursor)) as cursor:
        cursor.execute(sql, query_args)
        rows = cursor.fetchall()
        log.debug('Site results query complete',
              table=table.name,
              result_count=len(rows),
              result_dates=[(row['start_date'], row['end_date']) for row in rows],
              location_names=[row['nam_locn_name'] for row in rows])
        for row in rows:
            result_uuid = row['result_uuid']
            start_date = row['start_date']
            end_date = row['end_date']
            location_name = row['nam_locn_name']
            result = Result(result_uuid=result_uuid,
                            start_date=start_date,
                            end_date=end_date,
                            location_name=location_name)
            results.append(result)
    return results
