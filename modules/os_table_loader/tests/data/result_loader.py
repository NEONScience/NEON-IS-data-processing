from datetime import datetime

from os_table_loader.data.result_loader import Result
from os_table_loader.data.table_loader import Table


def get_results(_table: Table) -> list[Result]:
    """Mock function to return results for a maintenance table."""
    return [Result(result_uuid='934799d6-fe30-421f-87d7-89b4b8c95e73',
                   start_date=datetime(2022, 10, 11, 13, 19),
                   end_date=datetime(2022, 10, 11, 13, 19),
                   location_name='FLNT')]


def get_site_results(_table: Table, _site: str, _start_date: datetime, _end_date: datetime):
    """Mock function to return results for a maintenance table at a given site and time range."""
    return [Result(result_uuid='934799d6-fe30-421f-87d7-89b4b8c95e73',
                   start_date=datetime(2022, 10, 11, 13, 19),
                   end_date=datetime(2022, 10, 11, 13, 19),
                   location_name='FLNT')]
