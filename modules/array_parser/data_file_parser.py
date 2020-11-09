#!/usr/bin/env python3
import pyarrow.parquet as pq
import pandas as pd
import pyarrow as pa
from pathlib import Path
import structlog

import array_parser.schema_parser as schema_parser


log = structlog.getLogger()


def parse(path: Path, out_path: Path, schema: Path) -> None:
    """
    Reorder the data value array to columns with the appropriate schema field names.

    :param path: The data file path.
    :param out_path: The path to write the new file.
    :param schema: The new schema for the reordered file.
    :return: None
    """
    # source_id, site, timestamp, water_temp
    term_mapping = schema_parser.parse_schema(schema)
    table = pq.read_table(path)
    # new_table = pa.Table.from_arrays(arrays=, names=, schema=, metadata=)
    sources = table.column(0)
    sites = table.column(1)
    times = table.column(2)
    values = table.column(3)
    source_column = []
    site_column = []
    time_column = []
    term_names: list = list(term_mapping.mapping.values())
    print(f'term_names: {term_names}')
    value_columns = []

    i = 0
    while i < len(term_names):
        value_columns.append([])
        i = i + 1

    j = 0
    while j < table.num_rows:
        source = sources[j]
        site = sites[j]
        time = times[j]
        # print(f'{j}. source: {source} site: {site} time: {time}')
        source_column.append(source)
        site_column.append(site)
        time_column.append(time_column)
        k = 0
        while k < len(values[i]):
            # term_name = term_mapping.mapping.get(str(k))
            value = values[j][k]
            # print(f'term: {term_name} value: {value}')
            value_columns[k].append(value)
            k = k + 1
        j = j + 1

    column_names = table.column_names
    data = {'source_id': source_column, 'site': site_column, 'timestamp': time_column}
    z = 0
    while z < len(term_names):
        term_name = term_names[z]
        data[term_name] = value_columns[z]
        z = z + 1

    # fill extra columns with NaN.
    if len(column_names) < len(term_names):

    data_frame = pd.DataFrame(data=data)
    # table = pa.Table.

    # print(table.to_pandas())
    parquet_file = pq.ParquetFile(path)
    metadata = parquet_file.metadata
    print(f'metadata: {metadata}')

    # pq.write_table(table, path)

    # table = pa.Table.from_pandas(df, preserve_index=False, nthreads=1, schema=arrow_schema).replace_schema_metadata({
    #     'parquet.avro.schema': rawschema,
    #     'writer.model.name': 'avro'
    # })
    #
    # log.info(f"Writing parquet file: {out_path}")
    # pq.write_table(
    #     table,
    #     out_path,
    #     use_dictionary=dupcols,
    #     compression=codec,
    #     compression_level=5,
    #     coerce_timestamps='ms',
    #     allow_truncated_timestamps=False)
