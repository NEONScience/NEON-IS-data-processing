from contextlib import closing
from pathlib import Path

import csv
from typing import Any

from common.date_formatter import to_string
from os_table_loader.data.field_loader import Field
from os_table_loader.output.file_path import get_filename, get_filepath
from os_table_loader.table_data import TableData, ResultValues


class CsvTextBuilder:

    def __init__(self):
        self.csv_string = []

    def write(self, row):
        self.csv_string.append(row)


def write_file(out_path: Path, table_data: TableData) -> None:
    """Write a CSV file for each maintenance table."""
    file_name = get_filename(table=table_data.table, extension='csv')
    file_path = get_filepath(out_path, file_name)
    with closing(open(file_path, 'w', encoding='UTF8')) as file:
        writer = csv.writer(file)
        writer.writerow(get_header(table_data.fields))
        for result_values in table_data.results:
            writer.writerow(get_file_row(result_values))


def get_csv_text(table_data: TableData) -> str:
    """Return the CSV data as a string."""
    csv_file = CsvTextBuilder()
    writer = csv.writer(csv_file)
    writer.writerow(get_header(table_data.fields))
    for result_values in table_data.results:
        writer.writerow(get_file_row(result_values))
    csv_text = csv_file.csv_string
    return ''.join(csv_text)


def get_header(fields: list[Field]) -> list[str]:
    return [field.field_name for field in fields]


def get_file_row(result_values: ResultValues) -> list[Any]:
    row = []
    result = result_values.result
    for field_value in result_values.values:
        field_name = field_value.field.field_name
        if field_name == 'uid':
            row.append(result.result_uuid)
            continue
        if field_name == 'startDate':
            row.append(to_string(result.start_date))
            continue
        if field_name == 'endDate':
            row.append(to_string(result.end_date))
            continue
        result_value = field_value.value
        if result_value is not None:
            string_value = result_value.string_value
            number_value = result_value.number_value
            date_value = result_value.date_value
            uri_value = result_value.uri_value
            if string_value is not None:
                row.append(string_value)
            if number_value is not None:
                row.append(number_value)
            if date_value is not None:
                row.append(date_value)
            if uri_value is not None:
                row.append(uri_value)
        else:
             row.append(None)
    return row
