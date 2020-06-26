#!/usr/bin/env python3
from contextlib import closing

from cx_Oracle import Connection

from data_access.get_active_periods import get_active_periods


class ActivePeriodAssigner(object):

    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    def assign_active_periods(self, named_location_id: int, clone_id: int) -> None:
        """
        Get all the active periods associated with an existing named location
        and associate those periods with the cloned named location.

        :param named_location_id: The named location ID to assign.
        :param clone_id: The clone ID.
        """
        sql = '''
            insert into active_period (
                active_period_id, 
                named_location_id, 
                start_date, 
                end_date, 
                change_by, 
                tran_date
            )
            values (
                active_period_id_seq1.nextval, 
                :named_location_id, 
                :start_date, 
                :end_date, 
                :change_by, 
                CURRENT_TIMESTAMP
            )
        '''
        periods = get_active_periods(self.connection, named_location_id)
        with closing(self.connection.cursor()) as cursor:
            cursor.prepare(sql)
            for period in periods:
                start_date = period.start_date
                end_date = period.end_date
                cursor.execute(None,
                               named_location_id=clone_id,
                               start_date=start_date,
                               end_date=end_date,
                               change_by='water quality prototype')
                self.connection.commit()
