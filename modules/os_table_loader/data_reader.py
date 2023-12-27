from functools import partial
from typing import NamedTuple, Callable

from data_access.db_connector import DbConnector
from os_table_loader.field_loader import Field, get_fields
from os_table_loader.result_values_loader import ResultValue, get_result_values
from os_table_loader.result_loader import Result, get_results
from os_table_loader.table_loader import Table, get_tables


class DataReader(NamedTuple):
    get_tables: Callable[[str], list[Table]]
    get_fields: Callable[[Table], list[Field]]
    get_results: Callable[[Table], list[Result]]
    get_result_values: Callable[[Result], dict[int, ResultValue]]


def get_data_reader(connector: DbConnector) -> DataReader:
    get_tables_partial = partial(get_tables, connector)
    get_fields_partial = partial(get_fields, connector)
    get_results_partial = partial(get_results, connector)
    get_result_values_partial = partial(get_result_values, connector)
    return DataReader(get_tables=get_tables_partial,
                      get_fields=get_fields_partial,
                      get_results=get_results_partial,
                      get_result_values=get_result_values_partial)
