#!/usr/bin/env python3
from contextlib import closing
from typing import Dict, List, Set, Iterator, Optional, Tuple

from data_access.db_connector import DbConnector
from data_access.types.property import Property
from data_access.get_named_location_parents import get_named_location_parents


def get_group_loader_properties(connector: DbConnector, group_id: int) -> List[Property]:
    """
    Get the properties associated with a group id.

    :param connector: A database connection.
    :param group_id: The group ID to search.
    :return: The group properties.
    """
    sql = '''
       select
            g.group_name,
            g.hor,
            g.ver, 
            g.visibility_code, 
            nam_locn.nam_locn_id, 
            nam_locn.nam_locn_name
        from
            "group" g, nam_locn
        where
            nam_locn.nam_locn_id = g.named_location_id
        and
            g.group_id = %s
    '''
    properties: List[Property] = []
    hor_name = "HOR"
    ver_name = "VER"
    site_name = "site"
    domain_name = "domain"
    visibility_code_name = "VISIBILITY_CODE"
    connection = connector.get_connection()
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [group_id])
        rows = cursor.fetchall()
        for row in rows:
            # name = row[0]
            hor = row[1]
            ver = row[2]
            visibility_code = row[3]
            key = row[4]
            site = row[5]
            parents: Dict[str, Tuple[int, str]] = get_named_location_parents(connector, key)
            (parent_id, name_domain) = parents['domain'] if parents else None
            domain: str = name_domain
            properties.append(Property(name=site_name, value=site))
            properties.append(Property(name=domain_name, value=domain))
            properties.append(Property(name=hor_name, value=hor))
            properties.append(Property(name=ver_name, value=ver))
            properties.append(Property(name=visibility_code_name, value=visibility_code))
    return properties
