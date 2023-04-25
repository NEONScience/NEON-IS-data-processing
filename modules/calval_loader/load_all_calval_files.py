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
import cProfile
import pstats
from put_calibration_to_pachyderm import put_calibration_to_pachyderm
from get_avro_schema_name import get_avro_schema_name
from get_calibration_stream_name import get_calibration_stream_name
from google.cloud.storage import Client
from typing import NamedTuple, List
from typing import Dict
import datetime


class File(NamedTuple):
    source_url: str
    destination_path: Path


def get_db_url() -> str:
    env = environs.Env()
    return env('DATABASE_URL')


def load() -> None:
    env = environs.Env()
    ingest_bucket_name = env.str['CVAL_INGEST_BUCKET']
    in_path: Path = env.path('IN_PATH')

    credentials = json.loads(os.environ['GCP_PACHY_WRITER_JSONKEY'])
    gcp_client = Client.from_service_account_info(credentials)

    ingest_bucket = gcp_client.bucket(ingest_bucket_name)
   # path_names = gcp_client.list_blobs(ingest_bucket_name)
    print(f'Number of files in the bucke are ', path_names.__sizeof__())
   # repository_files: Dict[str, List[File]] = {}

            with pachyderm_client.commit("calibration_li191r", "master") as commit:
            with closing(psycopg2.connect(get_db_url())) as connection:
                print(f'connection is: {connection}')
                for key in path_names:
                    if key.name.endswith('.xml'):
                        print(f'key.name, {key.name}')
                        print('Key Found!')
                        filename = key.name
                        temp_file_path = f'/tmp/{filename}'
                        print('file path: ', temp_file_path)
                        try:
                            blob = ingest_bucket.get_blob(key.name)
                            blob.download_to_filename(temp_file_path)
                            tree = ET.parse(temp_file_path)
                            root = tree.getroot()
                            asset_id = root.find('SensorID').find('MxAssetID').text
                            print('asset_id is:', asset_id)
                            avro_schema_name = get_avro_schema_name(connection, asset_id)
                            print('avro_schema_name: ', avro_schema_name)
                            stream_id = root.find('StreamCalVal').find('StreamID').text
                            stream_name = get_calibration_stream_name(connection, avro_schema_name, stream_id)
                            print('Stream name is: ', stream_name)
                            python_pachyderm.put_files(pachyderm_client, temp_file_path, commit,
                                                       f'/{avro_schema_name}/{asset_id}/{stream_name}/{filename}')
                            print(f'copying source file from ingest bucket to longterm bucket {filename}')
                            ingest_bucket.copy_blob(blob, longterm_bucket, f'{filename}')
                            # delete in old destination
                            blob.delete()
                            print('deleting source file from ingest bucket.')
                            os.remove(temp_file_path)
                        except Exception as e:
                            print(f'Error is: {e.response["Error"]}')
                            if e.response['Error']['Code'] == '404':
                                log.error(f'File {filename} not found in object store.')
                            else:
                                log.error(f'Error from S3: {e.response["Error"]}')
                                continue
        pachyderm_client.finish_commit(("calibration_li191r", "master"))

    print(f'Calibration upload to pachyderm is finished')


if __name__ == '__main__':
    load()

