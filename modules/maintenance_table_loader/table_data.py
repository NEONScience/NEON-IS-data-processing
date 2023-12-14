from typing import NamedTuple, Optional

from maintenance_table_loader.field_loader import Field
from maintenance_table_loader.result_loader import Result
from maintenance_table_loader.result_values_loader import ResultValue
from maintenance_table_loader.table_loader import Table


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
