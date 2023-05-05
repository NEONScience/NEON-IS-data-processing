
from google.cloud import storage
from typing import Dict
from pathlib import Path
from contextlib import closing
import environs
import os
import xml.etree.ElementTree as ET
import json
import shutil
import glob
import sys
import datetime
import json
from google.cloud import storage


def calval_loader() -> None:

    ingest_bucket_name = os.environ['CVAL_INGEST_BUCKET']
    storage_client = storage.Client()
    ingest_bucket = storage_client.bucket(ingest_bucket_name)
    path_names = storage_client.list_blobs(ingest_bucket)
    files_to_be_uploaded = list()
    for blob in path_names:
        output_directory: Path = env.path('OUT_PATH')
        file_name = os.path.splitext(blob.name)[0]
        file_path = os.path.join(output_directory, file_name+".txt")         

        print("File name is:  ", file_path)
        cal_file= open(file_path, "w")
        cal_file.write(blob.updated.strftime("%Y-%m-%d %H:%M:%S"))
        cal_file.close()
        
if __name__ == '__main__':
    calval_loader()
