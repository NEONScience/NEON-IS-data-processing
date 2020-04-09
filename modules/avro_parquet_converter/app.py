import os

import environs
import structlog
import pathlib
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

from spavro.datafile import DataFileReader
from spavro.io import DatumReader

from lib import log_config as log_config
from lib import file_crawler as file_crawler

log = structlog.get_logger()


def convert(in_path, out_path):
    for avro_file_path in file_crawler.crawl(in_path):
        parts = avro_file_path.parts
        parquet_file_path = pathlib.Path(os.path.join(out_path, *parts[3:]))
        parquet_file_path.parent.mkdir(parents=True, exist_ok=True)
        open_file = open(avro_file_path, "rb")
        avro_data = DataFileReader(open_file, DatumReader())
        log.debug(avro_data.meta["avro.schema"].decode('utf-8'))
        data_frame = pd.DataFrame(data=avro_data)
        table = pa.Table.from_pandas(data_frame.astype({'readout_time': 'datetime64[ms]'})).replace_schema_metadata({
            'parquet.avro.schema': avro_data.meta["avro.schema"].decode('utf-8'),
            'writer.model.name': 'avro'
        })
        #  use_dictionary=False is file specific. PRT files are smaller with dictionary encoding off.
        #  Some files will be better with this on and others better with it off on a per column basis
        #  depending on how similar the records are in the column.
        pq.write_table(table, parquet_file_path, compression='gzip', use_dictionary=False,
                       compression_level=5, coerce_timestamps='ms', allow_truncated_timestamps=False)
        parquet_file_path.rename(parquet_file_path.with_suffix('.parquet'))


def main():
    env = environs.Env()
    in_path = env('IN_PATH')
    out_path = env('OUT_PATH')
    log_level = env('LOG_LEVEL')
    log_config.configure(log_level)
    convert(in_path, out_path)


if __name__ == '__main__':
    main()
