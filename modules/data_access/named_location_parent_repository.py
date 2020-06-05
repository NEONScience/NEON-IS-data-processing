#!/usr/bin/env python3
from contextlib import closing

import structlog


log = structlog.get_logger()


class NamedLocationParentRepository(object):
    """Class to represent a named location parent repository backed by a database."""

    def __init__(self, connection):
        self.connection = connection

    def get_parents(self, named_location_id: int):
        """
        Get the parents of a named location.

        :param named_location_id: A named location ID.
        :return: Parent data.
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
        parents = []
        with closing(self.connection.cursor()) as cursor:
            cursor.prepare(sql)
            self._add_parent(cursor, named_location_id, parents)
            return parents

    def _add_parent(self, cursor, named_location_id: int, parents: list):
        """
        Recursively add named location parents to the given list.

        :param cursor: A database cursor object.
        :type cursor: cursor object
        :param named_location_id: The location ID.
        :param parents: Collection of parents for appending.
        """
        res = cursor.execute(None, named_location_id=named_location_id)
        row = res.fetchone()
        if row is not None:
            parent_id = row[0]
            parent_name = row[1]
            type_name = row[2]
            if type_name.lower() == 'site':  # Only include the site.
                parents.append({'id': parent_id, 'name': parent_name, 'type': type_name})
            self._add_parent(cursor, parent_id, parents)
