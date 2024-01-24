
from google.cloud import storage
from pathlib import Path
import environs
import os
import sys
import re
from google.cloud import storage


def l0_data_loader() -> None:

    env = environs.Env()
    ingest_bucket_name = env.str('BUCKET_NAME')
    source_type = env.str('SOURCE_TYPE')
    output_directory: Path = env.path('OUT_PATH')
    storage_client = storage.Client()
    print(f"L0 Bucket name : {ingest_bucket_name}")
    ingest_bucket = storage_client.bucket(ingest_bucket_name)
    
    
    
    import_trigger: Path = env.path('import_trigger')
    pathname, extension = os.path.splitext(import_trigger)
    import_path = pathname.split('/')
    download_year = import_path[4]
    download_month = import_path[5]
    download_day = import_path[6]
    gen_date = download_year+"-"+download_month+"-"+download_day
    print("Gen_date is:  ", gen_date)
    
    blobs = list(ingest_bucket.list_blobs(prefix=f"v1/{source_type}/ms={download_year}-{download_month}"))
    for blob in blobs:
        print("blob name is:  ", blob.name)
        file_name = os.path.splitext(blob.name)[0]
        source_id =  re.split('_',re.split('/', file_name)[-1])[1]
        file_name = f"{source_type}_{source_id}_{gen_date}.parquet"
        file_path = Path(output_directory, source_type,download_year, download_month, download_day,source_id, "data",file_name )         
        print("File path is:  ", file_path)
        
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        l0_data_file= open(file_path, "w")
        l0_data_file.write(blob.updated.strftime("%Y-%m-%d %H:%M:%S"))
        l0_data_file.close()
        
if __name__ == '__main__':
    l0_data_loader()
