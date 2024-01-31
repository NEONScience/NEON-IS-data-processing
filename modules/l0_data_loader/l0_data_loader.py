
from google.cloud import storage
from pathlib import Path
import environs
import os
import sys
import re
from datetime import datetime
from google.cloud import storage


def l0_data_loader() -> None:

    env = environs.Env()
    ingest_bucket_name = env.str('BUCKET_NAME')
    source_type = env.str('SOURCE_TYPE')
    output_directory: Path = env.path('OUT_PATH')
    storage_client = storage.Client()
    #print(f"L0 Bucket name : {ingest_bucket_name}")
    ingest_bucket = storage_client.bucket(ingest_bucket_name)
    
    import_trigger: Path = env.path('import_trigger')
    pathname, extension = os.path.splitext(import_trigger)
    import_path = pathname.split('/')
    #print(f"impport_path is {import_path}")
    download_year = import_path[4]
    download_month = import_path[5]
    download_day = import_path[6]
    gen_date = download_year+"-"+download_month+"-"+download_day
    #print(f"gen_date is {gen_date}")
    
    blobs = list(ingest_bucket.list_blobs(prefix=f"v1/{source_type}/ms={download_year}-{download_month}"))
    for blob in blobs:
       # print("blob name is:  ", blob.name)
        file_name = os.path.splitext(blob.name)[0]
        source_id =  re.split('_',re.split('/', file_name)[-1])[1]
        file_date = re.split('_',re.split('/', file_name)[-1])[2]
        trigger_date = datetime(int(download_year), int(download_month), int(download_day))
        bucket_file_date = datetime(int(re.split('-', file_date)[0]), int(re.split('-', file_date)[1]), int(re.split('-', file_date)[2]))
        if(trigger_date == bucket_file_date):
            file_name = re.split('/', file_name)[-1] + ".parquet"
            file_path = Path(output_directory, source_type,download_year, download_month, download_day,source_id, "data",file_name )         
            print("File path is:  ", file_path)
            with blob.open("r") as f:
                file_path.parent.mkdir(parents=True, exist_ok=True)
                l0_data_file= open(file_path, "wb")
                l0_data_file.write(blob.download_as_string())
                l0_data_file.close()
if __name__ == '__main__':
    l0_data_loader()
