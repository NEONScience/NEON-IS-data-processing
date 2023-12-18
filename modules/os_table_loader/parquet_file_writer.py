import json
from pathlib import Path
from io import StringIO

import pandas
import pyarrow

from os_table_loader.csv_file_writer import get_csv_text
from os_table_loader.field_loader import Field
from os_table_loader.file_writer import get_filepath, get_filename
from os_table_loader.table_data import TableData


def write_file(out_path: Path, table_data: TableData) -> None:
    """Write a Parquet file for the given maintenance table."""
    file_name = get_filename(table_data.table, 'parquet')
    file_path = get_filepath(out_path, file_name)
    schema_json = get_avro_schema(table_data.fields)
    csv_text = get_csv_text(table_data)
    data_frame = pandas.read_csv(StringIO(csv_text), sep=',')
    table = pyarrow.Table.from_pandas(data_frame, preserve_index=False)
    # Add the Avro schema
    custom_metadata_bytes = json.dumps(schema_json).encode('utf8')
    existing_metadata = table.schema.metadata
    merged_metadata = {**{'Record Metadata': custom_metadata_bytes}, **existing_metadata}
    modified_table = table.replace_schema_metadata(merged_metadata)
    pyarrow.parquet.write_table(modified_table, file_path)


def get_avro_schema(fields: list[Field]) -> str:
    """Generate an Avro schema from the fields of the maintenance table."""
    schema_structure = {
        "namespace": "org.neonscience.avro",
        "type": "record",
        "fields": []
    }
    for field in fields:
        field_name = field.field_name
        data_type_code = field.data_type_code
        pub_format = field.pub_format
        schema_structure['fields'].append({'field_name': field_name,
                                           'field_type': data_type_code,
                                           'pub_format': pub_format})
    return json.dumps(schema_structure, indent=4)
