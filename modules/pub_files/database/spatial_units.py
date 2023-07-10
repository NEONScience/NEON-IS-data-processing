from contextlib import closing

from data_access.db_connector import DbConnector


def get_spatial_unit(connector: DbConnector, srid: int) -> str:
    """Get the spatial unit name for a particular spatial reference identifier."""
    schema = connector.get_schema()
    connection = connector.get_connection()
    sql = f'''
        select
            split_part((regexp_split_to_array(srtext, 'UNIT\["'))[array_length(regexp_split_to_array(srtext, 'UNIT\['), 1)], '"', 1) 
        as 
            unit 
        from 
            {schema}.spatial_ref_sys 
        where 
            srid = %s
    '''
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [srid])
        row = cursor.fetchone()
        unit=None
        if row is not None:
            unit = row[0]
        return unit
