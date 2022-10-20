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
             g.group_id, g.group_name, gm.member_group_id 
        from 
             "group" g, group_member gm, group_active_period gap 
        where
            g.group_id = gm.group_id 
        and
          	g.group_id = gap.group_id 
        and
            g.group_name like '%s%%'
    '''
    
    sql_mem_name = '''
        select
            g.group_name as member_name
        from 
            "group" g
        where
            g.group_id = %s
    '''
    
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, group_prefix)
        rows = cursor.fetchall()
        for row in rows:
            key = row[0]
            group_name = row[1]
            member_id = row[2]
            active_periods: List[ActivePeriod] = get_group_active_periods(connection, key)
            properties: List[Property] = get_group_properties(connection, key)
            groups = Group(name=name, group=group_name, active_periods=active_periods, properties=properties)
            yield groups
  
