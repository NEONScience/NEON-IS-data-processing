from typing import NamedTuple, Optional

from os_table_loader.data.field_loader import Field
from os_table_loader.data.result_loader import Result
from os_table_loader.data.result_values_loader import ResultValue
from os_table_loader.data.table_loader import Table


class FieldValue(NamedTuple):
    field: Field
    value: Optional[ResultValue]


class ResultValues(NamedTuple):
    result: Result
    values: list[FieldValue]


class TableData(NamedTuple):
    table: Table
    fields: list[Field]
    results: list[ResultValues]
