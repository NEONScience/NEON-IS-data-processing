#!/usr/bin/env python3
from contextlib import closing
from typing import List

import structlog

from data_access.named_location_parent import NamedLocationParent

log = structlog.get_logger()


class NamedLocationParentRepository(object):
    """Class to represent a named location parent repository backed by a database."""

    def __init__(self, connection):
        self.connection = connection
        self.site_type = 'site'

    def get_site(self, named_location_id: int) -> NamedLocationParent:
        """
        Get the site of a named location.

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
        with closing(self.connection.cursor()) as cursor:
            cursor.prepare(sql)
            self._find_site(cursor, named_location_id, parents)
        return parents[0]

    def _find_site(self, cursor, named_location_id: int, parents: List[NamedLocationParent]):
        """
        Recursively search for the site.

        :param cursor: A database cursor object.
        :param named_location_id: The named location ID.
        """
        result = cursor.execute(None, named_location_id=named_location_id)
        row = result.fetchone()
        if row is not None:
            parent_id = row[0]
            name = row[1]
            type_name = row[2]
            if type_name.lower() == self.site_type:
                parents.append(NamedLocationParent(id=parent_id, name=name, type=type_name))
            self._find_site(cursor, parent_id, parents)
