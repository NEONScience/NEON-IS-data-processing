#!/usr/bin/env python3
from contextlib import closing
from typing import List, Iterator

from data_access.db_connector import DbConnector
from data_access.types.active_period import ActivePeriod
from data_access.types.group import Group
from data_access.types.property import Property
from data_access.get_group_loader_properties import get_group_loader_properties
from data_access.get_group_loader_active_periods import get_group_loader_active_periods
from data_access.get_group_loader_group_id import get_group_loader_group_id


def get_group_loaders(connector: DbConnector, group_prefix: str) -> Iterator[Group]:
    """
    Get member groups for a group prefix, i.e., pressure-air_.

    :param connector: A database connector.
    :param group_prefix: A group prefix.
    :return: The Group.
    """
    sql_nlg = '''

         select distinct
             nlg.named_location_id as mem_id, nl.nam_locn_name  as mem_name
         from 
             named_location_group nlg, "group" g, nam_locn nl
         where 
             nlg.group_id = g.group_id
         and 
             nlg.named_location_id = nl.nam_locn_id 
         and 
             nlg.named_location_id in (select nl.nam_locn_id from nam_locn nl)
         and 
             g.group_name like %s

    '''

    sql_gm = '''

         select distinct 
             gm.member_group_id, g2.group_name as mem_name
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

    '''

    groups: List[Group] = []
    group_name: str = ""
    group_prefix_1: str = group_prefix[:-1] + '\_%'
    connection = connector.get_connection()
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql_nlg, [group_prefix_1])
        rows_nlg = cursor.fetchall()
        cursor.execute(sql_gm, [group_prefix_1])
        rows_gm = cursor.fetchall()
        rows = rows_nlg + rows_gm
        groups_all = []
        for row in rows:
            mem_id = row[0]
            mem_name = row[1]
            groups = []
            group_ids: List[int] = get_group_loader_group_id(connector, mem_id=mem_id)
            for group_id in group_ids:
                group_name: str = get_group_loader_group_name(connector, group_id=group_id, group_prefix_1=group_prefix_1)
                if group_name != "":
                    active_periods: List[ActivePeriod] = get_group_loader_active_periods(connector, group_id=group_id)
                    properties: List[Property] = get_group_loader_properties(connector, group_id=group_id)
                    groups.append(Group(name=mem_name, group=group_name, active_periods=active_periods, properties=properties))        
            groups.append(groups)
            groups_all.append(groups)
    return groups_all


def get_group_loader_group_name(connector: DbConnector, group_id: int, group_prefix_1: str) -> str:
    """
    Get group name for a group id.

    :param connector: A database connection.
    :param group_id: A group id.
    :return: The Group name.
    """  
    sql_group_name = '''
         select 
             g.group_name 
         from
            "group" g
         where 
             g.group_id = %s
         and 
         	 g.group_name like %s

    '''
    group_name: str = ''
    with closing(connector.get_connection().cursor()) as cursor:
        cursor.execute(sql_group_name, (group_id, group_prefix_1))
        rows = cursor.fetchall()
        for row in rows:
            group_name = row[0]
    return group_name
