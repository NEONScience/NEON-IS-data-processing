from contextlib import closing
from datetime import datetime
from typing import NamedTuple

import psycopg2.extras

from data_access.db_connector import DbConnector
from maintenance_table_loader.table_loader import Table


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
    '''
    with closing(connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)) as cursor:
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
