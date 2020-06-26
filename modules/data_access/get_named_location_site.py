#!/usr/bin/env python3
from typing import List, Optional, NamedTuple
from contextlib import closing

from cx_Oracle import Connection, Cursor


class NamedLocationParent(NamedTuple):
    id: int
    name: str
    type: str


def get_named_location_site(connection: Connection, named_location_id: int) -> Optional[str]:
    """
    Get the site of a named location.

    :param connection: A database connection.
    :param named_location_id: A named location ID.
    :return: The site.
    """
    sql = '''
        select
            prnt_nam_locn_id, nam_locn.nam_locn_name, type.type_name
        from
            nam_locn_tree
        join
            nam_locn on nam_locn.nam_locn_id = nam_locn_tree.prnt_nam_locn_id
        join
            type on type.type_id = nam_locn.type_id
        where
            chld_nam_locn_id = :named_location_id
    '''
    parents: List[NamedLocationParent] = []
    with closing(connection.cursor()) as cursor:
        cursor.prepare(sql)
        find_site(cursor, named_location_id, parents)
    if not parents:
        return None
    else:
        return parents[0].name


def find_site(cursor: Cursor, named_location_id: int, parents: List[NamedLocationParent]):
    """
    Recursively search for the site.

    :param cursor: A database cursor object.
    :param named_location_id: The named location ID.
    :param parents: Collection to append to.
    """
    result = cursor.execute(None, named_location_id=named_location_id)
    row = result.fetchone()
    if row is not None:
        parent_id = row[0]
        name = row[1]
        type_name = row[2]
        if type_name.lower() == 'site':
            parents.append(NamedLocationParent(id=parent_id, name=name, type=type_name))
        find_site(cursor, parent_id, parents)
