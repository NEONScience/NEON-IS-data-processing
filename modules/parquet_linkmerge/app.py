#!/usr/bin/env python3
import os
import sys
from pathlib import Path, PurePosixPath

import environs
import pyarrow
import pyarrow.parquet as pq
import structlog

from io import BytesIO
from collections.abc import Hashable

from lib import log_config
from lib import file_crawler

log = structlog.get_logger()


def write_merged_parquet(input_files, out_path):
    in_path = input_files[0]
    fp = open(in_path, 'rb')
    fio = BytesIO(fp.read())
    fp.close()
    # Use BytesIO to read the entire file into ram before using it with pyarrow
    # this way pyarrow can seek() the object if it's a named pipe and work
    tb1 = pq.read_table(fio)
    fio.close()
    tb1_schema = tb1.schema.metadata['parquet.avro.schema'.encode('UTF-8')] 
    df = tb1.to_pandas()
    for f in input_files[1:]:
        fp = open(f, 'rb')
        fio = BytesIO(fp.read())
        fp.close()
        tbf = pq.read_table(fio)
        fio.close()
        tbf_schema = tbf.schema.metadata['parquet.avro.schema'.encode('UTF-8')]
        if tbf_schema != tb1_schema:
            log.error(f"{f} schema does not match {in_path} schema")
            sys.exit(1)
        log.info(f"Merging {f} with {in_path}")
        df = df.append(tbf.to_pandas())

    df = df.sort_values('readout_time')
    # Check which columns in the data frame are hashable
    hashable_cols = [x for x in df.columns if isinstance(df[x][0], Hashable)]
    # For all the hashable columns, see if over 30% of the columns are duplicated
    dupcols = [x.encode('UTF-8') for x in hashable_cols if (df[x].duplicated().sum() / (int(df[x].size) - 1)) > 0.3]

    table = pyarrow.Table.from_pandas(df, preserve_index=False, nthreads=1).replace_schema_metadata({
        'parquet.avro.schema': tb1_schema,
        'writer.model.name': 'avro'
    })

    # Figure out the output filename
    inpath_new_filename = '_'.join(in_path.name.split('_')[1:])
    # Strip off / pfs /IN_PATH (3 parts)
    stripped_inpath = PurePosixPath().joinpath(*in_path.parts[3:])
    output_file_path = Path(out_path, stripped_inpath.parent, inpath_new_filename)
    if not output_file_path.parent.exists():
        log.info(f"{output_file_path.parent} directory not found, creating")
        os.makedirs(output_file_path.parent)

    log.info(f"Writing merged parquet file {output_file_path}")
    pq.write_table(
        table,
        output_file_path,
        use_dictionary=dupcols,
        compression='gzip',
        compression_level=5,
        coerce_timestamps='ms',
        allow_truncated_timestamps=False
    )


def link_merge(in_path, out_path):
    filedict = {}
    for parquet_file_path in file_crawler.crawl(in_path):
        file_source_id = parquet_file_path.name.split('_')[2]
        if file_source_id not in filedict:
            filedict[file_source_id] = [parquet_file_path]
        else:
            filedict[file_source_id].append(parquet_file_path)
    for source_id in filedict:
        # If there is only one file for the source ID, symlink it
        if len(filedict[source_id]) == 1:
            inpath = filedict[source_id][0]
            inpath_new_filename = '_'.join(inpath.name.split('_')[1:])
            # Strip off / pfs /IN_PATH (3 parts)
            stripped_inpath = PurePosixPath().joinpath(*inpath.parts[3:])
            outpath = Path(out_path, stripped_inpath.parent, inpath_new_filename)
            if not outpath.parent.exists():
                log.info(f"{outpath.parent} directory not found, creating")
                os.makedirs(outpath.parent)
            log.info(f"Linking {inpath} to {outpath}")
            os.symlink(inpath, outpath)
        else:
            write_merged_parquet(input_files=filedict[source_id], out_path=out_path)


def main():
    env = environs.Env()
    in_path = env.path('IN_PATH')
    out_path = env.path('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL', 'INFO')
    # default 30 percent duplication threshold
    dedup_threshold = env.float('DEDUP_THRESHOLD', 0.3)
    log_config.configure(log_level)
    link_merge(in_path, out_path)


if __name__ == '__main__':
    main()
