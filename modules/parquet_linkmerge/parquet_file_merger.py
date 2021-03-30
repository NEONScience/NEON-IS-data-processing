#!/usr/bin/env python3
import sys
from pathlib import Path
from typing import List

import pyarrow
import pyarrow.parquet as pq
import structlog

from io import BytesIO
from collections.abc import Hashable

from parquet_linkmerge.parquet_linkmerge_config import Config
from parquet_linkmerge.path_parser import PathParser

log = structlog.get_logger()


def link(path: Path, link_path: Path) -> None:
    if not link_path.exists():
        log.info(f"Linking {path} to {link_path}")
        link_path.symlink_to(path)


class ParquetFileMerger:

    def __init__(self, config: Config) -> None:
        self.in_path = config.in_path
        self.out_path = config.out_path
        self.duplication_threshold = config.duplication_threshold
        self.path_parser = PathParser(config)

    def merge(self) -> None:
        key_files = {}
        for path in self.in_path.rglob('*.parquet'):
            source_type, year, month, day, source_id = self.path_parser.parse(path)
            key = source_type+year+month+day+source_id
            if key not in key_files:
                key_files[key] = [path]
            else:
                key_files[key].append(path)
        for key in key_files:
            # Link if there is only one file for the key
            paths = key_files[key]
            if len(key_files[key]) == 1:
                path = key_files[key][0]
                link(path, self.to_output_path(path))
            else:
                site_keys = {}
                for path in paths:
                    site = path.name.split('_')[0]
                    if site not in site_keys:
                        site_keys[site] = [path]
                    else:
                        site_keys[site].append(path)
                if len(site_keys.keys()) > 1:  # there are different sites on the same day
                    files_to_merge: List[Path] = []
                    for site_key in site_keys:
                        files_to_merge.extend(site_keys[site_key])
                    self.write_merged_files(files_to_merge)
                else:
                    for site_key in site_keys:
                        paths = site_keys[site_key]
                        for path in paths:
                            link(path, self.to_output_path(path))

    def write_merged_files(self, input_files: List[Path]) -> None:
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
        output_file_path = self.to_output_path(path)
        log.info(f"writing merged parquet file {output_file_path}")
        pq.write_table(table,
                       output_file_path,
                       use_dictionary=duplicated_columns,
                       compression='gzip',
                       compression_level=5,
                       coerce_timestamps='ms',
                       allow_truncated_timestamps=False)

    def to_output_path(self, path: Path) -> Path:
        """
        Get a structured output path for an input path.

        :param path: A file path.
        :return: The output path.
        """
        filename = '_'.join(path.name.split('_')[1:])  # remove the site and first '_' from the filename
        source_type, year, month, day, source_id = self.path_parser.parse(path)
        output_path = Path(self.out_path, source_type, year, month, day, source_id, 'data', filename)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        return output_path
