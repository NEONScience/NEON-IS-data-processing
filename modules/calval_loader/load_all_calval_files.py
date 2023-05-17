#!/usr/bin/env python3
import os
import logging
from contextlib import closing
from pathlib import Path
import environs
import xml.etree.ElementTree as ET
import sys
from calval_loader.get_avro_schema_name import get_avro_schema_name
from calval_loader.get_calibration_stream_name import get_calibration_stream_name
from google.cloud import storage
from data_access.db_config_reader import read_from_mount
from data_access.db_connector import DbConnector
import datetime


def load() -> None:
    env = environs.Env()
    ingest_bucket_name = env.str('CVAL_INGEST_BUCKET')
    in_path: Path = env.path('IN_PATH')
    output_directory: Path = env.path('OUT_PATH')
    sensor_type = env.str('SOURCE_TYPE')
    db_config = read_from_mount(Path('/var/db_secret'))
    storage_client = storage.Client()
    ingest_bucket = storage_client.bucket(ingest_bucket_name)
    starting_path_index: int = env.int('STARTING_PATH_INDEX')
    with closing(DbConnector(db_config)) as connector:
        now = datetime.datetime.now()
        try:
            data_path_start = Path(*in_path.parts[0:starting_path_index+1]) # starting index
            print("Starting New Datum in the load_al_calval_files pipeline ")
            for path in data_path_start.rglob('*'):
                pathname, extension = os.path.splitext(path)
                filename = pathname.split('/')
                filename = filename[-1] + ".xml"
                print("FileName is: ", filename)
                blob = ingest_bucket.get_blob(filename)
    
                with blob.open("r") as f:
                    root = ET.fromstring(blob.download_as_string())
                    asset_id = root.find('SensorID').find('MxAssetID').text
                    avro_schema_name = get_avro_schema_name(connector.get_connection(), asset_id)
                    if ((avro_schema_name != None) and (avro_schema_name == sensor_type)):
                        stream_id = root.find('StreamCalVal').find('StreamID').text
                        stream_name = get_calibration_stream_name(connector.get_connection(), avro_schema_name, stream_id)
                        print('repo name , asset_id, stream_name, filename are :', avro_schema_name, "  ", asset_id, "  ",
                              stream_name, " ", filename)
                        try:
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
