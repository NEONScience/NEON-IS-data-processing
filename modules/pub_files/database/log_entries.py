from contextlib import closing
from datetime import datetime
from typing import List, NamedTuple

from data_access.db_connector import DbConnector


class LogEntry(NamedTuple):
    """Class to consolidate the log entry data."""
    change_log_id: int
    data_product_id: str
    issue_date: datetime
    resolution_date: datetime
    date_range_start: datetime
    date_range_end: datetime
    location_affected: str
    issue: str
    resolution: str


def get_log_entries(connector: DbConnector, data_product_id: str) -> List[LogEntry]:
    """Get the change log entries for the given data product identifier."""
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'''
         select
             dp_change_log_id,
             dp_idq,
             issue_date,
             resolved_date,
             date_range_start,
             date_range_end,
             location_affected,
             issue,
             resolution
         from
             {schema}.dp_change_log 
         where
             dp_idq = %s
        order by
             resolved_date desc, issue_date desc
    '''
    log_entries = []
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [data_product_id])
        rows = cursor.fetchall()
        for row in rows:
            change_log_id = row[0]
            data_product_id = row[1]
            issue_date = row[2]
            resolution_date = row[3]
            date_range_start = row[4]
            date_range_end = row[5]
            location_affected = row[6]
            issue = row[7]
            resolution = row[8]
            log_entry = LogEntry(change_log_id=change_log_id,
                                 data_product_id=data_product_id,
                                 issue_date=issue_date,
                                 resolution_date=resolution_date,
                                 date_range_start=date_range_start,
                                 date_range_end=date_range_end,
                                 location_affected=location_affected,
                                 issue=issue,
                                 resolution=resolution)
            if date_range_end is None:
                log_entries.insert(0, log_entry)
            else:
                log_entries.append(log_entry)
    return log_entries
