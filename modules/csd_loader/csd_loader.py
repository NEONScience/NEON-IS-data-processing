
from google.cloud import storage
from pathlib import Path
import environs
import os
import sys
from google.cloud import storage


def csd_loader() -> None:

    env = environs.Env()
    ingest_bucket_name = env.str('INGEST_BUCKET')
    gcs_root_prefix = env.str('GCS_ROOT_PREFIX', 'v2-dev').strip('/')
    csd_folder = env.str('CSD_FOLDER', 'csd_corrected').strip('/')
    output_directory: Path = env.path('OUT_PATH')
    storage_client = storage.Client()
    ingest_bucket = storage_client.bucket(ingest_bucket_name)
    target_prefix = f"{gcs_root_prefix}/{csd_folder}"
    path_names = storage_client.list_blobs(ingest_bucket, prefix=target_prefix)
    
    for blob in path_names:
        file_name = os.path.splitext(blob.name)[0]
        print("file_name is:  ", file_name)
        file_path = Path(output_directory, blob.updated.strftime("%Y"), blob.updated.strftime("%m"), blob.updated.strftime("%d"), file_name+".parquet")        
        file_path.parent.mkdir(parents=True, exist_ok=True)
        print("file_path is:  ", file_path)
        log_file= open(file_path, "w")
        log_file.write(blob.updated.strftime("%Y-%m-%d %H:%M:%S"))
        log_file.close()
        
if __name__ == '__main__':
    csd_loader()
