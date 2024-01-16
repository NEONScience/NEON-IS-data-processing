
from google.cloud import storage
from pathlib import Path
import environs
import os
import sys
import re
from google.cloud import storage


def l0_data_loader() -> None:

    env = environs.Env()
    ingest_bucket_name = env.str('l0-bucket')
    source_type = env.str('SOURCE_TYPE')
    output_directory: Path = env.path('OUT_PATH')
    storage_client = storage.Client()
    ingest_bucket = storage_client.bucket(ingest_bucket_name)
    gen_date = env.str('GEN_DATE')
    download_year = env.str('download_year')
    download_month = env.str('download_month')
    download_day = env.str('download_day')
    gen_date = env.str('GEN_DATE')
      
    path_names = storage_client.list_blobs(ingest_bucket, prefix="v1/{source_type}/ms={download_year}-{download_month}", delimiter='/')
    for blob in path_names:
        
        file_name = os.path.splitext(blob.name)[0]
        source_id =  re.split('_', file_name)[1]
        file_path = Path(output_directory, source_type,download_year, download_month, download_day,source_id, "data", {source_type}+"_"+{source_id}+"_"+{gen_date}+".parquet")         
        file_path.parent.mkdir(parents=True, exist_ok=True)
        print("File name is:  ", file_path)
        l0_data_file= open(file_path, "w")
        l0_data_file.write(blob.updated.strftime("%Y-%m-%d %H:%M:%S"))
        l0_data_file.close()
        
if __name__ == '__main__':
    l0_data_loader()
