#!/usr/bin/env python3
from contextlib import closing
from typing import List

from psycopg2 import extensions


def get_group_loader_group_id(connection: extensions.connection, mem_id: int) -> List[int]:
    """
    Get context entries for a named location.

    :param connection: A database connection.
    :param mem_id: The member group or named location ID.
    :return: The group IDs for the group member.
    """
    sql_1 = '''
         select
             g.group_id 
         from 
             named_location_group nlg, "group" g
         where
             nlg.group_id = g.group_id 
         and 
             nlg.named_location_id = %s
   	
    '''
          
    sql_2 = '''       
         select
             g.group_id 
         from 
             group_member gm, "group" g
         where
             gm.member_group_id = g.group_id 
         and 
             gm.member_group_id = %s
 	
    '''

    group_ids: List[int] = []
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql_1,[mem_id])
        rows_1 = cursor.fetchall()
        cursor.execute(sql_2,[mem_id])
        rows_2 = cursor.fetchall()
        rows = rows_1 + rows_2
        for row in rows:
            group_id = row[0]
            group_ids.append(group_id)
    return group_ids

