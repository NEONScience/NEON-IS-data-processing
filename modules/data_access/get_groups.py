#!/usr/bin/env python3
from contextlib import closing
from typing import List

from psycopg2 import extensions

from data_access.types.active_period import ActivePeriod
from data_access.types.group import Group
from data_access.types.property import Property
from data_access.get_group_properties import get_group_properties
from data_access.get_group_active_periods import get_active_periods


def get_groups(connection: extensions.connection,group_prefix: str) -> List[str]:
    """
    Get member groups for a group prefix, i.e., pressure-air_.

    :param connection: A database connection.
    :param group_prefix: A group prefix.
    :return: The Group.
    """
    sql_nlg = '''

 	    select
        	nlg.named_location_id as mem_id, nl.nam_locn_name  as mem_name, 
        	nlg.group_id as group_id, g.group_name as group_name
        from 
        	named_location_group nlg, "group" g, nam_locn nl
        where 
        	nlg.group_id = g.group_id
        and 
        	nlg.named_location_id = nl.nam_locn_id 
        and 
        	nlg.named_location_id in (select nl.nam_locn_id 
        from  
        	nam_locn nl)
        and 
        	g.group_name like %s
        group by nlg.named_location_id, nl.nam_locn_name, 
        	nlg.group_id, g.group_name

    '''
    sql_gm = '''

 	    select 
        	gm.member_group_id, g2.group_name as mem_name, 
        	g.group_id as group_id, g.group_name as group_name
        from 
        	group_member gm, "group" g, "group" g2
        where 
        	gm.group_id = g.group_id
        and 
        	gm.member_group_id = g2.group_id 
        and 
        	gm.member_group_id in (select g3.group_id 
        from  
        	"group" g3)
        and 
        	g.group_name like %s
        group by gm.member_group_id, g2.group_name, g.group_id, g.group_name

    '''

    group_names: List[str] = []
    group_ids: List[int] = []
    groups: List[Group] = []
    group_prefix_1: str = group_prefix + '%'
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql_nlg,[group_prefix_1])
        rows_nlg = cursor.fetchall()
        cursor.execute(sql_gm,[group_prefix_1])
        rows_gm = cursor.fetchall()
        rows = rows_nlg + rows_gm
        for row in rows:
            mem_id = row[0]
            mem_name = row[1]
            group_id = row[2]
            group_name = row[3]
            active_periods: List[ActivePeriod] = get_active_periods(connection,group_id=group_id)
            properties: List[Property] = get_group_properties(connection,group_id=group_id)
            groups = Group(name=mem_name,group=group_name,active_periods=active_periods,properties=properties)
            yield groups
