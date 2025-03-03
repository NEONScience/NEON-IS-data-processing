#!/usr/bin/env python3
from typing import Dict, Optional, Tuple, Any
from contextlib import closing

from data_access.db_connector import DbConnector


def get_named_location_parents(connector: DbConnector, named_location_id: int) -> Optional[Dict[str, Tuple[int, str]]]:
    """
    Get the site of a named location.

    :param connector: A database connection.
    :param named_location_id: A named location ID.
    :return: The site.
    """
    schema = connector.get_schema()
    connection = connector.get_connection()
    parents: Dict[str, Tuple[int, str]] = {}
    with closing(connection.cursor()) as cursor:
        find_parent(cursor, schema, named_location_id, parents)
    if not parents:
        return None
    else:
        return parents


def find_parent(cursor: Any, schema: str, named_location_id: int, parents: Dict[str, Tuple[int, str]]):
    """
    Recursively search for the site.

    :param cursor: A database cursor object.
    :param schema: The schema to query.
    :param named_location_id: The named location ID.
    :param parents: Collection to append to.
    """
    sql = f'''
        select
            prnt_nam_locn_id, 
            nam_locn.nam_locn_name, 
            type.type_name
        from
            {schema}.nam_locn_tree
        join
            {schema}.nam_locn on nam_locn.nam_locn_id = nam_locn_tree.prnt_nam_locn_id
        join
            {schema}.type on type.type_id = nam_locn.type_id
        where
            chld_nam_locn_id = %s
    '''
    cursor.execute(sql, [named_location_id])
    row = cursor.fetchone()
    if row is not None:
        parent_id = row[0]
        name = row[1]
        type_name = row[2]
        if type_name.lower() == 'site':
            parents['site'] = (parent_id, name)
        if type_name.lower() == 'domain':
            parents['domain'] = (parent_id, name)        
        find_parent(cursor, schema, parent_id, parents)
