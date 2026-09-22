from datetime import datetime
from functools import partial
from typing import NamedTuple, Callable

from data_access.db_connector import DbConnector
from os_table_loader.data.field_loader import Field, get_fields
from os_table_loader.data.result_values_loader import ResultValue, get_result_values
from os_table_loader.data.result_loader import Result, get_results, get_site_results
from os_table_loader.data.table_loader import Table, get_tables


class DataLoader(NamedTuple):
    get_tables: Callable[[str], list[Table]]
    get_fields: Callable[[Table], list[Field]]
    get_results: Callable[[Table], list[Result]]
    get_site_results: Callable[[Table, str, datetime, datetime], list[Result]]
    get_result_values: Callable[[Result], dict[int, ResultValue]]


def get_data_loader(connector: DbConnector) -> DataLoader:
    get_tables_partial = partial(get_tables, connector)
    get_fields_partial = partial(get_fields, connector)
    get_results_partial = partial(get_results, connector)
    get_site_results_partial = partial(get_site_results, connector)
    get_result_values_partial = partial(get_result_values, connector)
    return DataLoader(get_tables=get_tables_partial,
                      get_fields=get_fields_partial,
                      get_results=get_results_partial,
                      get_site_results=get_site_results_partial,
                      get_result_values=get_result_values_partial)
