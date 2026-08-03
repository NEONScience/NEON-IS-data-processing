#!/usr/bin/env python3

import os
from contextlib import closing
from pathlib import Path
import environs
import xml.etree.ElementTree as ET
import sys
from google.cloud import storage
from data_access.db_config_reader import read_from_mount
from data_access.db_connector import DbConnector
import datetime
import pandas as pd
import io
from io import BytesIO
from io import StringIO


def load() -> None:
    env = environs.Env()
    ingest_bucket_name = env.str('INGEST_BUCKET')
    in_path: Path = env.path('IN_PATH')
    print("IN_PATH value is:", in_path)
    output_directory: Path = env.path('OUT_PATH')
    db_config = read_from_mount(Path('/var/db_secret'))
    storage_client = storage.Client()
    ingest_bucket = storage_client.bucket(ingest_bucket_name)
    starting_path_index: int = env.int('STARTING_PATH_INDEX')
    print("starting_path_index value is:", starting_path_index)

    target_prefix = "BaM_beta"

    with closing(DbConnector(db_config)) as connector:
        now = datetime.datetime.now()
        try:
            data_path_start = Path(*in_path.parts[0:starting_path_index + 1])
            print("Starting New Datum in the load_bam pipeline", data_path_start)

            seen_paths: set[str] = set()
            placeholder_count = 0

            for path in data_path_start.rglob('*'):
                if not path.is_file():
                    continue

                path_string = path.as_posix()
                marker_index = path_string.find(target_prefix)
                if marker_index < 0:
                    continue

                gcs_path = path_string[marker_index:]
                if gcs_path in seen_paths:
                    continue

                seen_paths.add(gcs_path)
                placeholder_count += 1
                blob = ingest_bucket.blob(gcs_path)

                if not blob.exists():
                    print(f"WARNING: Blob does not exist in GCS: {gcs_path}")
                    continue

                output_path = Path(output_directory, gcs_path)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                blob.download_to_filename(str(output_path))
                print(f"Wrote file: {gcs_path} -> {output_path} ({output_path.stat().st_size} bytes)")

            # If no placeholder files were detected from IN_PATH, download all files
            # in the target GCS prefix so the loader can still complete.
            if placeholder_count == 0:
                print(f"No placeholder files found. Falling back to list_blobs on prefix: {target_prefix}")
                for blob in storage_client.list_blobs(ingest_bucket, prefix=target_prefix):
                    if blob.name.endswith('/'):
                        continue
                    output_path = Path(output_directory, blob.name)
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    blob.download_to_filename(str(output_path))
                    print(f"Wrote file: {blob.name} -> {output_path} ({output_path.stat().st_size} bytes)")

        except Exception as e:
            exception_type, exception_obj, exception_tb = sys.exc_info()
            print(f"FATAL ERROR at line {exception_tb.tb_lineno}: {exception_type.__name__}: {str(e)}")
            raise


if __name__ == '__main__':
    load()
