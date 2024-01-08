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
    ingest_bucket_name = env.str('LOGJAM_INGEST_BUCKET')
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
            print("Starting New Datum in the load_all_logjam_files pipeline ", data_path_start)
            for path in data_path_start.rglob('*'):
                if path.is_file():
                    pathname, extension = os.path.splitext(path)
                    print("pathname is: ", pathname)
                    path_split = pathname.split('/')
                    #print("path_split is: ", path_split)
                    
                    folder = path_split[-3]
                    print("folder is: ", folder)
                    
                    asset = path_split[-2]
                    print("asset is: ", asset)

                    filename = path_split[-1] + ".csv"
                    if filename == '.csv.csv':
                        print("Not a recognized file.")
                    else:
                        print("FileName is: ", filename)
                        gcs_path = os.path.join(folder, asset,filename)
                        print("gcs_path is: ", gcs_path)
                    
                        blob = ingest_bucket.blob(gcs_path)
                        try:
                            output_path = Path(output_directory,asset,filename)
                            print('Output Path is:', output_path)
                            output_path.parent.mkdir(parents=True, exist_ok=True)
                            print('Output Path is:', output_path)
                            with open(output_path, "wb") as output_file:
                                output_file.write(blob.download_as_string())
                            
                        except Exception:
                            exc_type, exc_obj, exc_tb = sys.exc_info()
                            print("Exception at line " + str(exc_tb.tb_lineno) + ": " + str(sys.exc_info()))

        except Exception:
            exception_type, exception_obj, exception_tb = sys.exc_info()
            print("Exception at line " + str(exception_tb.tb_lineno) + ": " + str(sys.exc_info()))


if __name__ == '__main__':
    load()
