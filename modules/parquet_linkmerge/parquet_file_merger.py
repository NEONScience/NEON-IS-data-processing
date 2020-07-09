#!/usr/bin/env python3
import sys
from pathlib import Path

import pyarrow
import pyarrow.parquet as pq
import structlog

from io import BytesIO
from collections.abc import Hashable

log = structlog.get_logger()


class ParquetFileMerger:

    def __init__(self, *, data_path: Path, out_path: Path, deduplication_threshold: float, relative_path_index: int):
        self.data_path = data_path
        self.out_path = out_path
        self.deduplication_threshold = deduplication_threshold
        self.relative_path_index = relative_path_index

    def write_merged_files(self, *, input_files, source_id):
        file_path = input_files[0]
        open_file = open(file_path, 'rb')
        fio = BytesIO(open_file.read())
        open_file.close()
        # Use BytesIO to read the entire file into ram before using it with pyarrow
        # this way pyarrow can seek() the object if it's a named pipe and work
        tb1 = pq.read_table(fio)
        fio.close()
        tb1_schema = tb1.schema.metadata['parquet.avro.schema'.encode('UTF-8')]
        df = tb1.to_pandas()
        for f in input_files[1:]:
            open_file = open(f, 'rb')
            fio = BytesIO(open_file.read())
            open_file.close()
            tbf = pq.read_table(fio)
            fio.close()
            tbf_schema = tbf.schema.metadata['parquet.avro.schema'.encode('UTF-8')]
            if tbf_schema != tb1_schema:
                log.error(f"{f} schema does not match {file_path} schema")
                sys.exit(1)
            log.info(f"Merging {f} with {file_path}")
            df = df.append(tbf.to_pandas())

        df = df.sort_values('readout_time')
        # Check which columns in the data frame are hashable
        hashable_columns = [x for x in df.columns if isinstance(df[x][0], Hashable)]
        # For all the hashable columns, see if duplicated columns are over the duplication threshold
        duplicated_columns = [x.encode('UTF-8') for x in hashable_columns
                              if (df[x].duplicated().sum() / (int(df[x].size) - 1)) > self.deduplication_threshold]

        table = pyarrow.Table.from_pandas(df, preserve_index=False, nthreads=1).replace_schema_metadata({
            'parquet.avro.schema': tb1_schema,
            'writer.model.name': 'avro'
        })
        filename = '_'.join(file_path.name.split('_')[1:])
        trimmed_path = Path(*file_path.parts[self.relative_path_index:])
        output_file_path = Path(self.out_path, trimmed_path.parent, source_id, filename)
        if not output_file_path.parent.exists():
            log.info(f"{output_file_path.parent} directory not found, creating")
            output_file_path.parent.mkdir(parents=True, exist_ok=True)
        log.info(f"Writing merged parquet file {output_file_path}")
        pq.write_table(
            table,
            output_file_path,
            use_dictionary=duplicated_columns,
            compression='gzip',
            compression_level=5,
            coerce_timestamps='ms',
            allow_truncated_timestamps=False
        )

    def merge(self):
        source_files = {}
        for path in self.data_path.rglob('*.parquet'):
            source_id = path.name.split('_')[2]
            if source_id not in source_files:
                source_files[source_id] = [path]
            else:
                source_files[source_id].append(path)
        for source_id in source_files:
            # Link if there is only one file for the source ID
            if len(source_files[source_id]) == 1:
                path = source_files[source_id][0]
                filename = '_'.join(path.name.split('_')[1:])
                trimmed_path = Path(*path.parts[self.relative_path_index:])
                link_path = Path(self.out_path, trimmed_path.parent, source_id, filename)
                if not link_path.parent.exists():
                    log.info(f"{link_path.parent} directory not found, creating")
                    link_path.parent.mkdir(parents=True, exist_ok=True)
                log.info(f"Linking {path} to {link_path}")
                link_path.symlink_to(path)
            else:
                self.write_merged_files(input_files=source_files[source_id], source_id=source_id)
