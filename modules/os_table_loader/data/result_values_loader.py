from contextlib import closing
from datetime import datetime
from typing import NamedTuple, Optional

import psycopg2.extras

from data_access.db_connector import DbConnector
from os_table_loader.data.result_loader import Result


class ResultValue(NamedTuple):
    result_uuid: str
    field_name: str
    rank: int
    string_value: Optional[str]
    number_value: Optional[float]
    date_value: Optional[datetime]
    uri_value: Optional[str]


def get_result_values(connector: DbConnector, result: Result) -> dict[int, ResultValue]:
    """Get the value for each Field in a Result and save by Field ID."""
    values = {}
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'''
        select
            os_result_data.pub_field_def_id,
            os_result_data.string_value,
            os_result_data.number_value, 
            os_result_data.date_value, 
            os_result_data.uri_value,
            pub_field_def.field_name,
            pub_field_def.rank
        from
            {schema}.os_result_data,
            {schema}.pub_field_def 
        where
            os_result_data.result_uuid = %(result_uuid)s
        and
            os_result_data.pub_field_def_id = pub_field_def.pub_field_def_id
        order by 
            pub_field_def.rank
    '''
    with closing(connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)) as cursor:
        cursor.execute(sql, dict(result_uuid=result.result_uuid))
        rows = cursor.fetchall()
        for row in rows:
            field_id = row['pub_field_def_id']
            string_value = row['string_value']
            number_value = row['number_value']
            date_value = row['date_value']
            uri_value = row['uri_value']
            field_name = row['field_name']
            rank = row['rank']
            value = ResultValue(result_uuid=result.result_uuid,
                                field_name=field_name,
                                rank=rank,
                                string_value=string_value,
                                number_value=number_value,
                                date_value=date_value,
                                uri_value=uri_value)
            values[field_id] = value
    return values
