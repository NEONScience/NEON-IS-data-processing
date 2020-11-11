#!/usr/bin/env python3
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path
import structlog
from typing import List

import array_parser.schema_parser as schema_parser
from array_parser.schema_parser import FieldMapping

log = structlog.getLogger()


def create_value_columns(field_names: List[str]) -> List[list]:
    """
    Create an empty column for each schema field name.

    :param field_names: The schema field names.
    :return: The empty columns.
    """
    value_columns: List[list] = []
    for n in range(0, len(field_names)):
        value_columns.append([])
    return value_columns


def get_schema(field_mapping: FieldMapping) -> pa.Schema:
    schema = pa.schema({
        'source_id': pa.string(),
        'site_id': pa.string(),
        'readout_time': pa.timestamp('ms'),
        'depth0WaterTemp': pa.float32(),
        'depth1WaterTemp': pa.float32(),
        'depth2WaterTemp': pa.float32(),
        'depth3WaterTemp': pa.float32(),
        'depth4WaterTemp': pa.float32(),
        'depth5WaterTemp': pa.float32(),
        'depth6WaterTemp': pa.float32(),
        'depth7WaterTemp': pa.float32(),
        'depth8WaterTemp': pa.float32(),
        'depth9WaterTemp': pa.float32(),
        'depth10WaterTemp': pa.float32()
    }).with_metadata({
        'parquet.avro.schema': field_mapping.schema,
        'writer.model.name': 'avro'
    })
    return schema


def parse(path: Path, out_path: Path, schema: Path) -> None:
    """
    Reorder the data value array to columns labelled with the appropriate schema field names.

    :param path: The data file path.
    :param out_path: The path to write the new file.
    :param schema: The new schema for the reordered file.
    :return: None
    """
    # source_id: string, site_id: string, readout_time: timestamp[ms], water_temperature list<item: float>
    field_mapping: FieldMapping = schema_parser.get_field_mapping(schema)
    table = pq.read_table(path)
    sources = table.column(0)
    sites = table.column(1)
    times = table.column(2)
    data_values = table.column(3)
    field_names: List[str] = list(field_mapping.mapping.values())
    value_columns: List[list] = create_value_columns(field_names)

    # loop over each table row, pull data values, and add values to the new columns
    for row_index in range(0, table.num_rows):
        # populate each value column with values from the file's 2D values array
        for value_index in range(0, len(field_names)):
            try:
                value = data_values[row_index][value_index].as_py()
            except IndexError:
                # There may be more field names than data array values, fill
                # extra columns with null data.
                value = None
            value_columns[value_index].append(value)

    data = {'source_id': sources, 'site_id': sites, 'readout_time': times}
    # add the new value columns to the table with schema field name keys
    for field_name_index in range(0, len(field_names)):
        field_name = field_names[field_name_index]
        column = value_columns[field_name_index]
        data[field_name] = column

    metadata = {
        'parquet.avro.schema': field_mapping.schema,
        'writer.model.name': 'avro'
    }
    new_table = pa.Table.from_pydict(data, metadata=metadata)  # schema=get_schema(field_mapping)

    print(f'names: {new_table.column_names}')
    print(f'new_table: {new_table}')

    # pq.write_table(new_table, path)

    parquet_file = pq.ParquetFile(path)
    metadata = parquet_file.metadata
    # print(f' metadata: {metadata}')
