#!/usr/bin/env python3
from contextlib import closing


class LocationTreeAssigner(object):

    def __init__(self, connection):
        self.connection = connection

    def get_parent(self, named_location_id: int):
        """
        Get the parent ID of a named location.

        :param named_location_id: The named location ID.
        :return: The parent ID
        """
        sql = 'select prnt_nam_locn_id from nam_locn_tree where chld_nam_locn_id = :id'
        with closing(self.connection.cursor()) as cursor:
            cursor.prepare(sql)
            cursor.execute(None, id=named_location_id)
            row = cursor.fetchone()
            parent_id = row[0]
        return parent_id

    def assign_parent(self, parent_id: int, child_id: int):
        """
        Assign a parent to a new named location ID.

        :param parent_id: The parent named location ID.
        :param child_id: The child named location ID.
        """
        sql = 'insert into nam_locn_tree (prnt_nam_locn_id, chld_nam_locn_id) values (:parent_id, :child_id)'
        with closing(self.connection.cursor()) as cursor:
            cursor.prepare(sql)
            cursor.execute(None, parent_id=parent_id, child_id=child_id)
            self.connection.commit()

    def assign_to_same_parent(self, source_named_location_id: int, assignable_named_location_id: int):
        """
        Assign a named location to the source named location.

        :param source_named_location_id: The source named location ID.
        :param assignable_named_location_id: The location ID to assign.
        """
        parent_id = self.get_parent(source_named_location_id)
        self.assign_parent(parent_id, assignable_named_location_id)

    def get_location_tree(self, parent_id: int, child_id: int):
        """
        Get the named location tree.

        :param parent_id: The parent named location ID.
        :param child_id: The child named location ID.
        :return: Matching parent and children IDs.
        """
        sql = '''
            select 
                prnt_nam_locn_id, chld_nam_locn_id 
            from 
                nam_locn_tree
            where 
                chld_nam_locn_id = :child_id and prnt_nam_locn_id = :parent_id
        '''
        entry = {}
        with closing(self.connection.cursor()) as cursor:
            cursor.prepare(sql)
            rows = cursor.execute(None, parent_id=parent_id, child_id=child_id)
            for row in rows:
                parent_id = row[0]
                child_id = row[1]
                entry = {'parent_id': parent_id, 'child_id': child_id}
        return entry
