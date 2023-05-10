from contextlib import closing

from data_access.db_connector import DbConnector


def get_type_name(connector: DbConnector, data_product_id: str) -> str:
    """Returns the type name of a data product."""
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'''
        select 
            type_name 
        from 
            {schema}."type" t, {schema}.dp_catalog 
        where 
            t.type_id = dp_catalog.type_id 
        and 
            dp_catalog.dp_idq = %s
    '''
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [data_product_id])
        row = cursor.fetchone()
        type_name = row[0]
    return type_name
