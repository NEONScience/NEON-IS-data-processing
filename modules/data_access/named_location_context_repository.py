#!/usr/bin/env python3
from contextlib import closing
from typing import List

import structlog


log = structlog.get_logger()


class NamedLocationContextRepository(object):
    """Class to represent a context repository backed by a database."""

    def __init__(self, connection):
        self.connection = connection

    def get_context(self, named_location_id: int) -> List[str]:
        """
        Get context entries for a named location.

        :param named_location_id: The named location ID.
        :return: The context entries.
        """
        sql = '''
            select 
                context_code, context_group_id
            from 
                nam_locn_context 
            where 
                nam_locn_id = :named_location_id
        '''
        with closing(self.connection.cursor()) as cursor:
            rows = cursor.execute(sql, named_location_id=named_location_id)
            contexts = []
            for row in rows:
                context_code = row[0]
                group = row[1]
                if group is None:
                    contexts.append(context_code)
                else:
                    group = f'{context_code}-{str(group)}'
                    contexts.append(group)
            return contexts
