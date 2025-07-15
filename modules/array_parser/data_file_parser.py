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
            except (IndexError,TypeError) as e:
                    # If the array is NULL or there are more field names than data values, fill extra columns with None.
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
    
    # Read the schema
    schema_data: SchemaData = schema_parser.parse_schema_file(schema)
    field_names = schema_data.field_names

    # Parse the array(s) into the new table
    table = pq.read_table(path)
    column_names = table.column_names
    array_names = set(schema_data.data_mapping.values())
    for array_name in array_names:
        column_index = column_names.index(array_name)
        data_values = table.column(column_index)
        array_field_names=[key for key, value in schema_data.data_mapping.items() if value == array_name] # field names pertaining to this array
        parsed_columns: List[list] = create_columns(array_field_names)
        data_type: pa.lib.ListType = data_values.type
        populate_columns(table, array_field_names, data_values, parsed_columns)
        
        # convert to arrays with the appropriate type
        for i in range(0, len(parsed_columns)):
            column: pa.Array = pa.array(parsed_columns[i], data_type.value_type)
            table: pa.Table = table.append_column(array_field_names[i], column)  # add column to table
    
    # remove original data arrays from table
    for array_name in array_names:
        column_names = table.column_names
        column_index = column_names.index(array_name)
        table = table.remove_column(column_index)  
        
    # Rearrange columns to match the parsed schema   
    table=table.select(field_names)
    metadata = get_metadata(schema_data)
    table = table.replace_schema_metadata(metadata)
    log.debug(f'modified_table:\n{table}')
    
    # Output
    file_path = Path(out_path, path.name)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.touch()
    pq.write_table(table, file_path)
