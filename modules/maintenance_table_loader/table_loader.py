from contextlib import closing
from typing import NamedTuple, Optional

import psycopg2.extras

from data_access.db_connector import DbConnector


class Table(NamedTuple):
    id: int
    data_product: str
    source_data_product: str
    name: str
    description: str
    usage: str
    table_type: str
    ingest_table_id: int
    filter_sample_class: Optional[str]


def get_tables(connector: DbConnector) -> list[Table]:
    tables = []
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'''
        select 
            pub_table_def_id,
            dp_idq,
            dp_idq_source,
            name,
            description,
            usage,
            table_type,
            ingest_table_def_id,
            filter_sample_class
        from 
            {schema}.pub_table_def 
        where 
            name like '%maintenance%'
        and 
            (usage = 'publication' or usage = 'both')
    '''
    with closing(connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)) as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()
        for row in rows:
            row_id = row['pub_table_def_id']
            data_product = row['dp_idq']
            source_data_product = row['dp_idq_source']
            name = row['name']
            description = row['description']
            usage = row['usage']
            table_type = row['table_type']
            ingest_table_id = row['ingest_table_def_id']
            filter_sample_class = row['filter_sample_class']
            table = Table(id=row_id,
                          data_product=data_product,
                          source_data_product=source_data_product,
                          name=name,
                          description=description,
                          usage=usage,
                          table_type=table_type,
                          ingest_table_id=ingest_table_id,
                          filter_sample_class=filter_sample_class)
            tables.append(table)
    return tables
