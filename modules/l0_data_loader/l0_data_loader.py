
from google.cloud import storage
from pathlib import Path
import environs
import os
import sys
import re
from google.cloud import storage


def l0_data_loader() -> None:

    print("Inside the l0_data_loader")
    env = environs.Env()
    ingest_bucket_name = env.str('BUCKET_NAME')
    source_type = env.str('SOURCE_TYPE')
    output_directory: Path = env.path('OUT_PATH')
    #ingest_bucket_name = 'neon-dev-l0-ingest'
    #source_type = 'li191r'
    #output_directory: Path = env.path('/pfs/out')
    #output_directory: Path = '/pfs/out'
    storage_client = storage.Client()
    ingest_bucket = storage_client.bucket(ingest_bucket_name)
    
    
    import_trigger: Path = env.path('import_trigger')
    pathname, extension = os.path.splitext(import_trigger)
    import_path = pathname.split('/')
    download_year = import_path[3]
    print("Download year is:  ", download_year)
    download_month = import_path[4]
    print("Download Month is:  ", download_month)
    download_day = import_path[5]
    print("Download Day is:  ", download_day)
    gen_date = download_year+"-"+download_month+"-"+download_day
    #download_year = env.str('download_year')
    #download_month = env.str('download_month')
    #download_day = env.str('download_day')
    #download_year = '2023'
    #download_month = '10'
    #download_day = '01'
   # gen_date = '2023-10-01'
      
    path_names = storage_client.list_blobs(ingest_bucket, prefix="v1/{source_type}/ms={download_year}-{download_month}", delimiter='/')
    for blob in path_names:
        
        file_name = os.path.splitext(blob.name)[0]
        # print("File name is:  ", file_name)
        source_id =  re.split('_', file_name)[1]
        file_path = Path(output_directory, source_type,download_year, download_month, download_day,source_id, "data", {source_type}+"_"+{source_id}+"_"+{gen_date}+".parquet")         
        file_path.parent.mkdir(parents=True, exist_ok=True)
        print("File path is:  ", file_path)
      #  l0_data_file= open(file_path, "w")
      #  l0_data_file.write(blob.updated.strftime("%Y-%m-%d %H:%M:%S"))
      # l0_data_file.close()
        
if __name__ == '__main__':
    l0_data_loader()
