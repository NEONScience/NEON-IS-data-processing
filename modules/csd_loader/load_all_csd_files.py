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
    
    with closing(DbConnector(db_config)) as connector:
        now = datetime.datetime.now()
        try:
            data_path_start = Path(*in_path.parts[0:starting_path_index + 1])  # starting index
            print("Starting New Datum in the load_all_csd_files pipeline ", data_path_start)
            for path in data_path_start.rglob('*'):
                if not path.is_file():
                    continue
                    
                try:
                    pathname, extension = os.path.splitext(path)
                    print("pathname is: ", pathname)
                    path_split = pathname.split('/')
                    print("path_split is: ", path_split)
                    
                    # Extract path components
                    year = path_split[-6]
                    month = path_split[-5]
                    day = path_split[-4]
                    group = path_split[-3]
                    folder = path_split[-2]
                    base_filename = path_split[-1]
                    
                    print(f"Date: {year}/{month}/{day}, Group: {group}, Folder: {folder}")
                    
                    # Construct filename - always use .parquet extension
                    filename = f"{base_filename}.parquet"
                    
                    # Skip if the base filename is empty
                    if not base_filename or base_filename == '.':
                        print(f"Skipping invalid filename: {path}")
                        continue
                    
                    print(f"Target filename: {filename}")
                    
                    # Construct GCS path
                    gcs_path = os.path.join(year, month, day, group, folder, filename)
                    print(f"GCS path: {gcs_path}")
                    
                    # Download from GCS
                    blob = ingest_bucket.blob(gcs_path)
                    print(f"blob is: {blob}")
                    
                    if not blob.exists():
                        print(f"WARNING: Blob does not exist in GCS: {gcs_path}")
                        continue
                    
                    # Create output path and directory
                    output_path = Path(output_directory, year, month, day, group, folder, filename)
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    print(f'Output path: {output_path}')
                    
                except IndexError as e:
                    print(f"ERROR: Path structure incorrect for {path}: {e}")
                    continue
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    print(f"ERROR at line {exc_tb.tb_lineno}: {exc_type.__name__}: {str(e)}")
                    print(f"Failed to process file: {path}")
                    continue

        except Exception as e:
            exception_type, exception_obj, exception_tb = sys.exc_info()
            print(f"FATAL ERROR at line {exception_tb.tb_lineno}: {exception_type.__name__}: {str(e)}")
            raise


if __name__ == '__main__':
    load()
