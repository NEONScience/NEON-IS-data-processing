
from google.cloud import storage
from pathlib import Path
import environs
import os
import sys
import re
from datetime import datetime
from google.cloud import storage


def l0_gcs_loader() -> None:

    env = environs.Env()
    ingest_bucket_name = env.str('BUCKET_NAME')
    bucket_version_path = env.str('BUCKET_VERSION_PATH') # The root path of the bucket, indicative of the version (e.g. v2)
    source_type_index = env.int('SOURCE_TYPE_INDEX',None) # One of SOURCE_TYPE_INDEX or SOURCE TYPE is required. SOURCE_TYPE_INDEX supercedes if both are input
    source_type = env.str('SOURCE_TYPE',None)
    year_index = env.int('YEAR_INDEX')
    month_index = env.int('MONTH_INDEX')
    day_index = env.int('DAY_INDEX')
    output_directory: Path = env.path('OUT_PATH')
    storage_client = storage.Client()
    #print(f"L0 Bucket name : {ingest_bucket_name}")
    ingest_bucket = storage_client.bucket(ingest_bucket_name)
    
    import_trigger: Path = env.path('import_trigger')
    pathname, extension = os.path.splitext(import_trigger)
    import_path = pathname.split('/')
    #print(f"impport_path is {import_path}")
    
    if (source_type_index is None) & (source_type is None):
        sys.exit("One of SOURCE_TYPE_INDEX or SOURCE_TYPE environment variables is required.")
    elif source_type_index is not None:
        source_type = import_path[source_type_index]
        
    download_year = import_path[year_index]
    download_month = import_path[month_index]
    download_day = import_path[day_index]
    gen_date = download_year+"-"+download_month+"-"+download_day
    #print(f"gen_date is {gen_date}")
    
    blobs = list(ingest_bucket.list_blobs(prefix=f"{bucket_version_path}/{source_type}/ms={download_year}-{download_month}"))
    for blob in blobs:
       # print("blob name is:  ", blob.name)
        file_path_bucket = os.path.splitext(blob.name)[0]
        file_name_bucket = re.split('/', file_path_bucket)[-1]
        source_id = re.split('/', file_path_bucket)[-2]
        source_id = source_id.replace("source_id=","")
        file_date = re.search('[0-9]{4}-[0-1]{1}[0-9]{1}-[0-3]{1}[0-9]{1}',file_name_bucket).group(0)
        trigger_date = datetime(int(download_year), int(download_month), int(download_day))
        bucket_file_date = datetime(int(re.split('-', file_date)[0]), int(re.split('-', file_date)[1]), int(re.split('-', file_date)[2]))
        if(trigger_date == bucket_file_date):
            file_name = file_name_bucket + ".parquet"
            file_path = Path(output_directory, source_type,download_year, download_month, download_day,source_id, "data",file_name )         
            print("File path is:  ", file_path)
            with blob.open("r") as f:
                file_path.parent.mkdir(parents=True, exist_ok=True)
                l0_data_file= open(file_path, "wb")
                l0_data_file.write(blob.download_as_string())
                l0_data_file.close()
if __name__ == '__main__':
    l0_gcs_loader()
