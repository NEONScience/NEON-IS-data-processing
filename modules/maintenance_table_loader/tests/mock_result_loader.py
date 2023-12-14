from datetime import datetime

from maintenance_table_loader.result_loader import Result
from maintenance_table_loader.table_loader import Table


def get_results(_table: Table) -> list[Result]:
    """Mock function to return the results for a maintenance table."""
    return [Result(result_uuid='934799d6-fe30-421f-87d7-89b4b8c95e73',
                   start_date=datetime(2022, 10, 11, 13, 19),
                   end_date=datetime(2022, 10, 11, 13, 19),
                   location_name='FLNT')]
