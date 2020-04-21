#!/usr/bin/env python3
import os
import sys

import environs
import pathlib
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import structlog

from collections.abc import Hashable
from fastavro import reader, is_avro

from lib import log_config as log_config
from lib import file_crawler as file_crawler

log = structlog.get_logger()


def convert(in_path, out_path, dedup_threshold):
    for avro_file_path in file_crawler.crawl(in_path):
        log.info(f"Opening Avro file {avro_file_path}")
        if not is_avro(str(avro_file_path)):
            log.error(f"error: {avro_file_path} is not an Avro file")
            sys.exit(1)

        with open(avro_file_path, "rb") as open_file:
            avro_data = reader(open_file)
            # Get the ordered list of field names from the avro schema
            avro_file_schema = avro_data.metadata['avro.schema']
            log.debug(f"avro_file_schema: {avro_file_schema}")
            avro_schema = avro_data.writer_schema
            log.debug(f"avro_schema: {avro_schema}")

            # Read Avro file into Pandas dataframe
            data_frame = pd.DataFrame(
                data=avro_data,
                # Preserve column ordering
                columns=[x['name'] for x in avro_schema['fields']]
            )
            log.debug(f"Data Frame info: {data_frame}")
        # Get a list of columns with hashable types
        log.debug(f"All Columns: {[x for x in data_frame.columns]}")
        hashable_cols = [x for x in data_frame.columns if isinstance(data_frame[x][0], Hashable)]
        log.debug(f"Hashable columns from the data_frame: {hashable_cols}")
        # Find columns with high duplication (> 30%) for use with dictionary encoding
        dupcols = [x.encode('UTF-8') for x in hashable_cols
                   if (data_frame[x].duplicated().sum() / (int(data_frame[x].size) - 1)) > dedup_threshold]
        log.debug(f"Columns to dedup: {dupcols}")
        table = pa.Table.from_pandas(data_frame).replace_schema_metadata({
            'parquet.avro.schema': avro_file_schema,
            'writer.model.name': 'avro'
        })
        parts = avro_file_path.parts
        parquet_file_path = pathlib.Path(os.path.join(out_path, *parts[3:]))
        parquet_file_path.parent.mkdir(parents=True, exist_ok=True)
        parquet_file_path = os.path.splitext(parquet_file_path)[0] + '.parquet'
        log.info(f"Writing parquet file: {parquet_file_path}")
        pq.write_table(table, parquet_file_path, compression='gzip', use_dictionary=dupcols,
                       compression_level=5, coerce_timestamps='ms', allow_truncated_timestamps=False)


def main():
    env = environs.Env()
    in_path = env.str('IN_PATH')
    out_path = env.str('OUT_PATH')
    log_level = env.str('LOG_LEVEL', 'INFO')
    # 30 percent duplication threshold for dedup by default
    dedup_threshold = env.float('DEDUP_THRESHOLD', 0.3)
    log_config.configure(log_level)
    convert(in_path, out_path, dedup_threshold)


if __name__ == '__main__':
    main()
