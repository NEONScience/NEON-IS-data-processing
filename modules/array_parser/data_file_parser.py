#!/usr/bin/env python3
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path
import structlog
from typing import List

import array_parser.schema_parser as schema_parser
from array_parser.schema_parser import SchemaData

log = structlog.getLogger()


def get_metadata(schema_data: SchemaData) -> dict:
    """Get the metadata for the new file."""
    return {'parquet.avro.schema': schema_data.schema, 'writer.model.name': 'avro'}


def create_columns(field_names: List[str]) -> List[list]:
    """
    Create an empty column for each schema field name.

    :param field_names: The schema data.
    :return: The empty columns.
    """
    columns: List[list] = []
    for n in range(0, len(field_names)):
        columns.append([])
    return columns


def populate_columns(table: pa.Table, field_names: List[str],
                     data_array: pa.ChunkedArray, new_columns: List[list]) -> None:
    """
    Add data values from the original data array to the new columns.

    :param table: The original table.
    :param field_names: The schema field names.
    :param data_array: The original data array.
    :param new_columns: The new empty columns.
    :return: None
    """
    # loop over each table row, pull data values, and add them to the new columns
    for row_index in range(0, table.num_rows):
        for field_name_index in range(0, len(field_names)):
            try:
                # get values from the file's 2D data array
                value = data_array[row_index][field_name_index].as_py()
            except IndexError:
                # If there are more field names than data values, fill extra columns with None.
                value = None
            # populate new columns
            new_columns[field_name_index].append(value)


def write_restructured_file(path: Path, out_path: Path, schema: Path) -> None:
    """
    Reorder the data value array to columns labelled with the appropriate schema field names
    and write the new file.

    :param path: The data file path.
    :param out_path: The path to write the new file.
    :param schema: The new schema for the reordered file.
    :return: None
    """
    table = pq.read_table(path)
    data_values = table.column(3)
    data_type: pa.lib.ListType = data_values.type
    schema_data: SchemaData = schema_parser.parse_schema_file(schema)
    field_names = schema_data.field_names
    new_columns: List[list] = create_columns(field_names)
    populate_columns(table, field_names, data_values, new_columns)
    for i in range(0, len(new_columns)):
        # convert to arrays with the appropriate type
        column: pa.Array = pa.array(new_columns[i], data_type.value_type)
        table: pa.Table = table.append_column(field_names[i], column)  # add column to table
    table = table.remove_column(3)  # remove original data array from table
    metadata = get_metadata(schema_data)
    table = table.replace_schema_metadata(metadata)
    log.debug(f'modified_table:\n{table}')
    file_path = Path(out_path, path.name)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.touch()
    pq.write_table(table, file_path)
