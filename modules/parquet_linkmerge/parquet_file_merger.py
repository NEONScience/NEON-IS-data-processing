#!/usr/bin/env python3
import sys
from pathlib import Path
from typing import List

import pyarrow
import pyarrow.parquet as pq
import structlog

from io import BytesIO
from collections.abc import Hashable

log = structlog.get_logger()


class ParquetFileMerger:

    def __init__(self, *, data_path: Path, out_path: Path, duplication_threshold: float, relative_path_index: int):
        self.data_path = data_path
        self.out_path = out_path
        self.duplication_threshold = duplication_threshold
        self.relative_path_index = relative_path_index

    def merge(self) -> None:
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
                link_path = self._convert_path(path)
                if not link_path.exists():
                    log.info(f"Linking {path} to {link_path}")
                    link_path.symlink_to(path)
            else:
                self._write_merged_files(input_files=source_files[source_id])

    def _write_merged_files(self, *, input_files: List[Path]) -> None:
        """
        Merge files from same source at different sites occurring on the same day.

        :param input_files: File paths organized by source ID.
        """
        path = input_files[0]
        open_file = open(path, 'rb')
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
                log.error(f"{f} schema does not match {path} schema")
                sys.exit(1)
            log.info(f"Merging {f} with {path}")
            df = df.append(tbf.to_pandas())
        df = df.sort_values('readout_time')
        # Check which columns in the data frame are hashable
        hashable_columns = [x for x in df.columns if isinstance(df[x][0], Hashable)]
        # For all the hashable columns, see if duplicated columns are over the duplication threshold
        duplicated_columns = [x.encode('UTF-8') for x in hashable_columns
                              if (df[x].duplicated().sum() / (int(df[x].size) - 1)) > self.duplication_threshold]
        table = pyarrow.Table.from_pandas(df, preserve_index=False, nthreads=1).replace_schema_metadata({
            'parquet.avro.schema': tb1_schema,
            'writer.model.name': 'avro'
        })
        output_file_path = self._convert_path(path)
        log.info(f"writing merged parquet file {output_file_path}")
        pq.write_table(
            table,
            output_file_path,
            use_dictionary=duplicated_columns,
            compression='gzip',
            compression_level=5,
            coerce_timestamps='ms',
            allow_truncated_timestamps=False
        )

    def _convert_path(self, path: Path) -> Path:
        """
        Generate the output path for a path.

        :param path: A file path.
        :return: The output path.
        """
        filename = '_'.join(path.name.split('_')[1:])
        parts = path.parts
        trimmed_path = Path(*parts[self.relative_path_index:len(parts)-1])
        output_path = Path(self.out_path, trimmed_path, filename)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        return output_path
