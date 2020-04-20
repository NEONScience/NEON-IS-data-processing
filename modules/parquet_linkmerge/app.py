#!/usr/bin/env python3
import os
import sys

import environs
import pathlib
import pyarrow
import pyarrow.parquet as pq
import structlog

from collections.abc import Hashable

from lib import log_config
from lib import file_crawler

log = structlog.get_logger()


def write_merged_parquet(inputfiles, in_path, out_path):
    inpath = inputfiles[0]
    fp = open(inpath, 'rb')
    # Use pyarrow.input_stream() to allow us to read from named pipes
    tb1 = pq.read_table(pyarrow.input_stream(fp))
    fp.close()
    tb1_schema = tb1.schema.metadata['parquet.avro.schema'.encode('UTF-8')] 
    df = tb1.to_pandas()
    for f in inputfiles[1:]:
        fp = open(f, 'rb')
        tbf = pq.read_table(pyarrow.input_stream(fp))
        fp.close()
        tbf_schema = tbf.schema.metadata['parquet.avro.schema'.encode('UTF-8')]
        if tbf_schema != tb1_schema:
            log.error(f"{f} schema does not match {inpath} schema")
            sys.exit(1)
        log.info(f"Merging {f} with {inpath}")
        df = df.append(tbf.to_pandas())

    df = df.sort_values('readout_time')
    # Check which columns in the dataframe are hashable
    hashable_cols = [x for x in df.columns if isinstance(df[x][0], Hashable)]
    # For all the hashable columns, see if over 30% of the columns are duplicated
    dupcols = [x.encode('UTF-8') for x in hashable_cols if (df[x].duplicated().sum() / (int(df[x].size) - 1)) > 0.3]

    table = pyarrow.Table.from_pandas(df, preserve_index=False, nthreads=1).replace_schema_metadata({
        'parquet.avro.schema': tb1_schema,
        'writer.model.name': 'avro'
    })

    # Figure out the output filename
    inpath_new_filename = '_'.join(inpath.name.split('_')[1:])
    # Strip off / pfs /IN_PATH (3 parts)
    stripped_inpath = pathlib.PurePosixPath().joinpath(*inpath.parts[3:])
    outpath = pathlib.Path(os.path.join(out_path, stripped_inpath.parent, inpath_new_filename))
    if not os.path.exists(outpath.parent):
        log.info(f"{outpath.parent} directory not found, creating")
        os.makedirs(outpath.parent)

    log.info(f"Writing merged parquet file {outpath}")
    pq.write_table(
        table,
        outpath,
        use_dictionary=dupcols,
        compression='gzip',
        compression_level=5,
        coerce_timestamps='ms',
        allow_truncated_timestamps=False
    )


def linkmerge(in_path, out_path, dedup_threshold):
    filedict = {}
    for parquet_file_path in file_crawler.crawl(in_path):
        file_source_id = parquet_file_path.name.split('_')[2]
        if file_source_id not in filedict:
            filedict[file_source_id] = [parquet_file_path]
        else:
            filedict[file_source_id].append(parquet_file_path)
    for source_id in filedict:
        # If there is only one file for the sourceid, we just symlink it
        if len(filedict[source_id]) == 1:
            inpath = filedict[source_id][0]
            inpath_new_filename = '_'.join(inpath.name.split('_')[1:])
            # Strip off / pfs /IN_PATH (3 parts)
            stripped_inpath = pathlib.PurePosixPath().joinpath(*inpath.parts[3:])
            outpath = pathlib.Path(os.path.join(out_path, stripped_inpath.parent, inpath_new_filename))
            if not os.path.exists(outpath.parent):
                log.info(f"{outpath.parent} directory not found, creating")
                os.makedirs(outpath.parent)
            log.info(f"Linking {inpath} to {outpath}")
            os.symlink(inpath, outpath)
        else:
            write_merged_parquet(inputfiles=filedict[source_id], in_path=in_path, out_path=out_path)


def main():
    env = environs.Env()
    in_path = env.str('IN_PATH')
    out_path = env.str('OUT_PATH')
    log_level = env.str('LOG_LEVEL', 'INFO')
    # 30 percent duplication threshold for dedup by default
    dedup_threshold = env.float('DEDUP_THRESHOLD', 0.3)
    log_config.configure(log_level)
    linkmerge(in_path, out_path, dedup_threshold)


if __name__ == '__main__':
    main()
