#!/usr/bin/env python3
from contextlib import closing
from typing import List

from cx_Oracle import Connection

import structlog


log = structlog.get_logger()


class ThresholdContextRepository(object):
    """Class to represent a threshold context repository backed by a database."""

    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    def get_context(self, condition_uuid: str) -> List[str]:
        """
        Get all context entries for a threshold.

        :param condition_uuid: The condition UUID.
        :return: The context codes.
        """
        context_codes: List[str] = []
        with closing(self.connection.cursor()) as cursor:
            query = 'select context_code from condition_context where condition_uuid = :condition_uuid'
            rows = cursor.execute(query, condition_uuid=condition_uuid)
            for row in rows:
                context_codes.append(row[0])
        return context_codes
