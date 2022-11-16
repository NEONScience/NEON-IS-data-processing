#!/usr/bin/env python3
from contextlib import closing
from typing import List

from data_access.db_connector import DbConnector


def get_group_loader_group_id(connector: DbConnector, mem_id: int) -> List[int]:
    """
    Get Group IDs for a named location.

    :param connector: A database connection.
    :param member_id: The member group or named location ID.
    :return: The group IDs for the group member.
    """
    sql_1 = '''
         select distinct
             gm.group_id 
         from 
             named_location_group nlg, "group_member" gm
         where
             nlg.group_id = gm.group_id 
         and 
             nlg.named_location_id = %s
   	
    '''
          
    sql_2 = '''       
         select
             g.group_id 
         from 
             group_member gm, "group" g
         where
             gm.group_id = g.group_id 
         and 
             gm.member_group_id = %s
 	
    '''

    group_ids: List[int] = []
    connection = connector.get_connection()
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql_1, [mem_id])
        rows_1 = cursor.fetchall()
        cursor.execute(sql_2, [mem_id])
        rows_2 = cursor.fetchall()
        rows = rows_1 + rows_2
        for row in rows:
            group_id = row[0]
            group_ids.append(group_id)
    return group_ids
