#!/usr/bin/env python3
import os
import logging
import io
from contextlib import closing
from pathlib import Path

import python_pachyderm
import environs
import psycopg2
import xml.etree.ElementTree as ET
import json
import shutil
import glob
import sys
from calval_loader.get_avro_schema_name import get_avro_schema_name
from calval_loader.get_calibration_stream_name import get_calibration_stream_name

from google.cloud import storage
import google.auth
from typing import NamedTuple, List
from typing import Dict
from data_access.db_config_reader import read_from_mount
from data_access.db_connector import DbConnector
import datetime
import urllib.request
import python_pachyderm


def load() -> None:
    env = environs.Env()
    ingest_bucket_name = env.str('CVAL_INGEST_BUCKET')
    in_path: Path = env.path('IN_PATH')
    db_config = read_from_mount(Path('/var/db_secret'))
    storage_client = storage.Client()
    ingest_bucket = storage_client.bucket(ingest_bucket_name)
    print("Bucet name is : ", ingest_bucket)
      pachyderm_client = python_pachyderm.Client
    print("Pachyderm client is: ", pachyderm_client)
    with closing(DbConnector(db_config)) as connector:
        now = datetime.datetime.now()
        try:
            pathname, extension = os.path.splitext(in_path)
            filename = pathname.split('/')
            filename = filename[-1] + ".xml"
            # print("FileName is: ", filename)

            blob = ingest_bucket.get_blob(filename)
            with blob.open("r") as f:
                root = ET.fromstring(blob.download_as_string())
                asset_id = root.find('SensorID').find('MxAssetID').text
                avro_schema_name = get_avro_schema_name(connector.get_connection(), asset_id)
                if (avro_schema_name != None):
                    stream_id = root.find('StreamCalVal').find('StreamID').text
                    stream_name = get_calibration_stream_name(connector.get_connection(), avro_schema_name, stream_id)
                    print('repo name , asset_id, stream_name, filename are :', avro_schema_name, "  ", asset_id, "  ",
                          stream_name, " ", filename)
                    try:
                        # file_url = f'https://storage.cloud.google.com/{ingest_bucket_name}/{filename}'
                        output_path = Path(output_directory, avro_schema_name, asset_id, stream_name, filename)
                        output_path.parent.mkdir(parents=True, exist_ok=True)
                        # print('Output Path is:', output_path)
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
