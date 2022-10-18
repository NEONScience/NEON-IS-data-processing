#!/usr/bin/env python3
from contextlib import closing
from typing import List

from psycopg2 import extensions

from data_access.types.active_period import ActivePeriod
from data_access.types.property import Property
from data_access.get_named_location_active_periods import get_active_periods

def get_groups(connection: extensions.connection, group_prefix: str) -> List[str]:
    """
    Get groups for a group prefix, i.e., pressure-air_.

    :param connection: A database connection.
    :param group_prefix: A group prefix.
    :return: The groups.
    """
    sql = '''
        select
            nlg.named_location_id, nl.nam_locn_name, g.group_name
        from 
            named_location_group nlg, "group" g, nam_locn nl
        where
            nlg.group_id = g.group_id 
        and 
            nlg.named_location_id = nl.nam_locn_id
        and 
            g.group_name like '%s%%'"
    '''

    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, group_prefix)
        rows = cursor.fetchall()
        for row in rows:
            key = row[0]
            name = row[1]
            group_name = row[2]
            active_periods: List[ActivePeriod] = get_active_periods(connection, key)
            properties: List[Property] = get_named_location_properties(connection, key)
            groups = Group(name=name, group=group_name, active_periods=active_periods, properties=properties)
            yield groups
  
