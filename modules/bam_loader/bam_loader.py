
from google.cloud import storage
from pathlib import Path
import environs


def bam_loader() -> None:

    env = environs.Env()
    ingest_bucket_name = env.str('INGEST_BUCKET')
    output_directory: Path = env.path('OUT_PATH')
    storage_client = storage.Client()
    ingest_bucket = storage_client.bucket(ingest_bucket_name)
    target_prefix = "v2-dev/BaM_beta"
    path_names = storage_client.list_blobs(ingest_bucket, prefix=target_prefix)

    for blob in path_names:
        # Ignore prefix placeholders that represent directories.
        if blob.name.endswith("/"):
            continue

        print("blob_name is:", blob.name)
        file_path = Path(
            output_directory,
            blob.updated.strftime("%Y"),
            blob.updated.strftime("%m"),
            blob.updated.strftime("%d"),
            blob.name,
        )
        file_path.parent.mkdir(parents=True, exist_ok=True)
        print("file_path is:", file_path)
        with open(file_path, "w", encoding="utf-8") as log_file:
            log_file.write(blob.updated.strftime("%Y-%m-%d %H:%M:%S"))
        
if __name__ == '__main__':
    bam_loader()
