#!/usr/bin/env python3
from contextlib import closing
from typing import List

import structlog

import common.date_formatter as date_formatter
from data_access.active_period import ActivePeriod

log = structlog.get_logger()


class ActivePeriodRepository(object):

    def __init__(self, connection):
        self.connection = connection

    def get_active_periods(self, named_location_id: int, cutoff_date=None) -> List[ActivePeriod]:
        """
        Get the active time periods for a named location.

        :param named_location_id: A named location ID.
        :param cutoff_date: The end time to limit periods with no set end time.
        :return: The active periods.
        """
        sql = 'select start_date, end_date from active_period where named_location_id = :id'
        with closing(self.connection.cursor()) as cursor:
            cursor.prepare(sql)
            rows = cursor.execute(None, id=named_location_id)
            periods: List[ActivePeriod] = []
            for row in rows:
                start_date = row[0]
                if start_date is not None:
                    end_date = row[1]
                    if cutoff_date is not None:
                        if end_date is None:
                            end_date = cutoff_date
                        elif end_date > cutoff_date:
                            end_date = cutoff_date
                    if start_date is not None:
                        start_date = date_formatter.convert(start_date)
                    if end_date is not None:
                        end_date = date_formatter.convert(end_date)
                    periods.append(ActivePeriod(start_date=start_date, end_date=end_date))
        return periods
