import functions_framework
import python_pachyderm
from flask import Response
from google.cloud import logging
import socket
#from google.cloud.storage import Client
from google.cloud import storage
from get_avro_schema_name import get_avro_schema_name
from get_calibration_stream_name import get_calibration_stream_name
import environs
import os
from contextlib import closing
import psycopg2
import xml.etree.ElementTree as ET
import json
import shutil
import glob
import sys



@functions_framework.cloud_event
def calval_file_upload_to_pachyderm(cloud_event):
    secret_locations = '/secrets/calibration_li191r-robot-token'
    with open(secret_locations) as f:
        AUTH_TOKEN = f.readlines()[0]
    # print('SECRET READ BEFORE::: ', AUTH_TOKEN)

    PACH_HOST = os.environ['PACH_HOST']

    pachyderm_client = python_pachyderm.Client(
        host=PACH_HOST,
        port=443,
        tls=True,
        auth_token=AUTH_TOKEN
    )
    gcp_client = storage.Client()

    data = cloud_event.data
    ingest_bucket = gcp_client.get_bucket(data['bucket'])
    filename = data['name']
    blob = ingest_bucket.get_blob(data['name'])
    #contents = blob.download_as_string()
    temp_file_path = f'/tmp/{filename}'
    print('temp_file path: ', temp_file_path)

    with closing(psycopg2.connect(get_db_url())) as connection:
        if filename.endswith('.xml'):
            try:
                blob.download_to_filename(temp_file_path)
                xml_string  = blob.download_as_string()
                print("xml_string is: ", xml_string)
                root = ET.fromstring(xml_string)
                asset_id = root.find('SensorID').find('MxAssetID').text
                print('asset_id is:', asset_id)
                avro_schema_name = get_avro_schema_name(connection, asset_id)
                if (avro_schema_name == None):
                    print(f'Continuing with the next schema');
                    os.remove(temp_file_path)
                    exit()
                print('avro_schema_name: ', avro_schema_name)
                stream_id = root.find('StreamCalVal').find('StreamID').text
                stream_name = get_calibration_stream_name(connection, avro_schema_name, stream_id)

                repo_name = avro_schema_name + "_calibration"
                print(f'Repo Name is: {repo_name}')
                try:
                    with pachyderm_client.commit(repo_name, "master") as commit:
                        python_pachyderm.put_files(pachyderm_client, temp_file_path, commit, f'/{avro_schema_name}/{asset_id}/{stream_name}/{filename}')
                    pachyderm_client.finish_commit((repo_name, "master"))
                except Exception:
                    print(f"exception caught while putting file to pachyderm")
                    exception_type, exception_obj, exception_tb = sys.exc_info()
                    print("Exception at line " + str(exception_tb.tb_lineno) + ": " + str(sys.exc_info()))
                try:
                    if os.path.exists(temp_file_path):
                        os.remove(temp_file_path)
                    else:
                        print(f"The file {temp_file_path} does not exist")
                except OSError as error:
                    print(error)
                    print("File path can not be removed")

            except Exception:
                if os.path.exists(temp_file_path):
                    os.remove(temp_file_path)
                exception_type, exception_obj, exception_tb = sys.exc_info()
                print("Exception at line " + str(exception_tb.tb_lineno) + ": " + str(sys.exc_info()))




def get_db_url() -> str:
    env = environs.Env()
    return env('DATABASE_URL')
