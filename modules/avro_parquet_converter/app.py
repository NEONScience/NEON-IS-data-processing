import os

import environs
import pathlib
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import spavro.schema
import structlog

from spavro.datafile import DataFileReader
from spavro.io import DatumReader

from lib import log_config as log_config
from lib import file_crawler as file_crawler

log = structlog.get_logger()


def convert(in_path, out_path, dedup_threshold):
    for avro_file_path in file_crawler.crawl(in_path):
        avro_data = {}
        with open(avro_file_path, "rb") as open_file:
            avro_data = DataFileReader(open_file, DatumReader())
            log.debug(avro_data.meta["avro.schema"].decode('utf-8'))
            # Get the ordered list of field names from the avro schema
            avro_file_schema = avro_data.get_meta('avro.schema')
            avro_schema = spavro.schema.parse(avro_file_schema)
            # Read Avro file into Pandas dataframe
            data_frame = pd.DataFrame(
                data=avro_data,
                # Preserve column ordering
                columns=[x.name for x in avro_schema.fields]
            ).astype({
                # Cast readout_time to correct pandas time type
                'readout_time': 'datetime64[ms]'
            })
        # Find columns with high duplication (> 30%) for use with dictionary encoding
        dupcols = [x.encode('UTF-8') for x in data_frame.columns
                   if (data_frame[x].duplicated().sum() / (int(data_frame[x].size) - 1)) > dedup_threshold]
        table = pa.Table.from_pandas(data_frame).replace_schema_metadata({
            'parquet.avro.schema': avro_file_schema,
            'writer.model.name': 'avro'
        })
        parts = avro_file_path.parts
        parquet_file_path = pathlib.Path(os.path.join(out_path, *parts[3:]))
        parquet_file_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, parquet_file_path, compression='gzip', use_dictionary=dupcols,
                       compression_level=5, coerce_timestamps='ms', allow_truncated_timestamps=False)
        parquet_file_path.rename(parquet_file_path.with_suffix('.parquet'))


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
