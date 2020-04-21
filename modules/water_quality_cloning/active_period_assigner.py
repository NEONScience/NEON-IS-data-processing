#!/usr/bin/env python3
from contextlib import closing

import lib.date_formatter as date_formatter
import data_access.named_location_finder as named_location_finder


def assign_active_periods(connection, named_location_id, clone_id):
    """
    Get all the active periods associated with an existing named location
    and associate those periods with the cloned named location.
    :param connection:
    :param named_location_id:
    :param clone_id:
    :return:
    """
    sql = '''
        insert into active_period
            (active_period_id, named_location_id, start_date, end_date, change_by, tran_date)
        values
            (active_period_id_seq1.nextval, :named_location_id, :start_date, :end_date, :change_by, CURRENT_TIMESTAMP)
    '''
    periods = named_location_finder.get_active_periods(connection, named_location_id)
    for period in periods:
        start_date = period.get('start_date')
        end_date = period.get('end_date')
        with closing(connection.cursor()) as cursor:
            cursor.prepare(sql)
            cursor.execute(None,
                           named_location_id=clone_id,
                           start_date=date_formatter.parse(start_date),
                           end_date=date_formatter.parse(end_date),
                           change_by='water quality prototype')
            connection.commit()
