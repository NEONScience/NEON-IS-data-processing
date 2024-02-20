from pathlib import Path

import structlog

from os_table_loader.data.data_loader import DataLoader
from os_table_loader.data.field_loader import Field
from os_table_loader.data.table_loader import Table
from os_table_loader.output.parquet_file_writer import write_file as write_parquet
from os_table_loader.output.csv_file_writer import write_file as write_csv
from os_table_loader.data.result_values_loader import ResultValue
from os_table_loader.data.result_loader import Result
from os_table_loader.table_data import ResultValues, FieldValue, TableData


log = structlog.get_logger()


def write_files(out_path: Path, data_loader: DataLoader, file_type: str, partial_table_name: str) -> None:
    """Write a file for each maintenance table whose name includes the partial table name."""
    for table in data_loader.get_tables(partial_table_name):
        fields: list[Field] = data_loader.get_fields(table)
        results: list[Result] = data_loader.get_results(table)
        if results:
            table_data = get_table_data(table, fields, results, data_loader)
            write_file(out_path, table_data, file_type)


def get_table_data(table: Table, fields: list[Field], results: list[Result],
                   data_loader: DataLoader) -> TableData:
    """Get the table results and result values from the data loader."""
    table_results: list[ResultValues] = []
    for result in results:
        values_by_field_id: dict[[int], ResultValue] = data_loader.get_result_values(result)
        field_values: list[FieldValue] = []
        for field in fields:
            try:
                result_value: ResultValue = values_by_field_id[field.field_id]
                field_values.append(FieldValue(field=field, value=result_value))
            except KeyError:
                field_values.append(FieldValue(field=field, value=None))
        table_results.append(ResultValues(result=result, values=field_values))
    return TableData(table=table, fields=fields, results=table_results)


def write_file(out_path: Path, table_data: TableData, file_type: str) -> None:
    """Write a file containing the table data into the specified output path."""
    if file_type == 'csv':
        write_csv(out_path, table_data)
    elif file_type == 'parquet':
        write_parquet(out_path, table_data)
    else:
        log.error(f'Output file type "{file_type}" not recognized.')
        raise SystemExit(1)
