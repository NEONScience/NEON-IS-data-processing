#!/usr/bin/env python3
from contextlib import closing

import common.date_formatter as date_formatter
from data_access.active_period_repository import ActivePeriodRepository


def assign_active_periods(connection, named_location_id, clone_id):
    """
    Get all the active periods associated with an existing named location
    and associate those periods with the cloned named location.

    :param connection: A database connection.
    :type connection: connection object
    :param named_location_id: The named location ID to assign.
    :type named_location_id: int
    :param clone_id: The clone ID.
    :type clone_id: int
    :return:
    """
    sql = '''
        insert into active_period
            (active_period_id, named_location_id, start_date, end_date, change_by, tran_date)
        values
            (active_period_id_seq1.nextval, :named_location_id, :start_date, :end_date, :change_by, CURRENT_TIMESTAMP)
    '''
    active_period_repository = ActivePeriodRepository(connection)
    periods = active_period_repository.get_active_periods(named_location_id)
    for period in periods:
        start_date = period.get('start_date')
        end_date = period.get('end_date')

        if start_date is not None:
            start_date = date_formatter.parse(start_date)
        if end_date is not None:
            end_date = date_formatter.parse(end_date)

        with closing(connection.cursor()) as cursor:
            cursor.prepare(sql)
            cursor.execute(None,
                           named_location_id=clone_id,
                           start_date=start_date,
                           end_date=end_date,
                           change_by='water quality prototype')
            connection.commit()
