from contextlib import closing
from datetime import datetime
from typing import List, NamedTuple

from data_access.db_connector import DbConnector


class Value(NamedTuple):
    """Class to hold the data for a single value."""
    id: int
    list_code: str
    name: str
    rank: int
    code: str
    effective_date: datetime
    end_date: datetime
    publication_code: str
    description: str


def get_value_list(connector: DbConnector, list_name: str) -> List[Value]:
    """Returns the value list for the given list name."""
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'''
        select
             lov_item_id,
             lov_code,
             name,
             rank,
             item_code,
             effective_date,
             end_date,
             pub_code,
             description
        from 
            {schema}.os_lov_item
        where 
            lov_code = %s
    '''
    value_list = []
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [list_name])
        for row in cursor.fetchall():
            row_id = row[0]
            list_code = row[1]
            name = row[2]
            rank = row[3]
            code = row[4]
            effective_date = row[5]
            end_date = row[6]
            publication_code = row[7]
            description = row[8]
            value = Value(id=row_id,
                          list_code=list_code,
                          name=name,
                          rank=rank,
                          code=code,
                          effective_date=effective_date,
                          end_date=end_date,
                          publication_code=publication_code,
                          description=description)
            value_list.append(value)
    return value_list
