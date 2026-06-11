
"""Load L0 parquet files from GCS using manifest-driven path selectors.

Manifest input can be provided by either:
1. MANIFEST: JSON-formatted string.
2. MANIFEST_FILE: Path to a file containing JSON.

If both are set, MANIFEST is used.

Accepted JSON formats for either input:
1. JSON object containing a "paths" array.
2. JSON array of strings.

Example object form:
{
    "paths": [
        "cmp22/2025/10/01/11185",
        "cmp22/2025/10/02/11185",
        "cmp22/2025/10/03",
        "cmp22/2026"
    ]
}

Example array form:
[
    "cmp22/2025/10/01/11185",
    "cmp22/2025/10/02/11185",
    "cmp22/2025/10/03",
    "cmp22/2026"
]

Each string in the manifest is split on '/'. Index environment variables map
positions in that split path:
- SOURCE_TYPE_INDEX
- YEAR_INDEX
- MONTH_INDEX
- DAY_INDEX
- SOURCE_ID_INDEX

Paths may be partial. Only one path element is required. Missing indexed
elements are treated as wildcards for listing/filtering, except SOURCE_TYPE,
which must be available from SOURCE_TYPE_INDEX for each processed path.

When SOURCE_ID_INDEX is present, the bucket prefix includes:
{BUCKET_VERSION_PATH}/{source_type}/ms={download_year}-{download_month}/source_id={source_id}
"""

from google.cloud import storage
from pathlib import Path
import environs
import os
import sys
import re
import json
from datetime import datetime


def _parse_manifest_data(manifest_data: object) -> list[str]:
    if isinstance(manifest_data, dict):
        manifest_paths = manifest_data.get('paths', [])
    elif isinstance(manifest_data, list):
        manifest_paths = manifest_data
    else:
        sys.exit('Manifest must contain a JSON array or an object with a "paths" array.')

    if not isinstance(manifest_paths, list):
        sys.exit('Manifest "paths" entry must be a JSON array of strings.')

    return [path for path in manifest_paths if isinstance(path, str) and path.strip()]


def l0_gcs_loader() -> None:

    env = environs.Env()
    ingest_bucket_name = env.str('BUCKET_NAME')
    bucket_version_path = env.str('BUCKET_VERSION_PATH')
    source_type_index = env.int('SOURCE_TYPE_INDEX', None)
    source_type_out = env.str('SOURCE_TYPE_OUT', None)
    year_index = env.int('YEAR_INDEX', None)
    month_index = env.int('MONTH_INDEX', None)
    day_index = env.int('DAY_INDEX', None)
    source_id_index = env.int('SOURCE_ID_INDEX', None)
    manifest_inline = env.str('MANIFEST', None)
    manifest_file_raw = env.str('MANIFEST_FILE', None)
    output_directory: Path = env.path('OUT_PATH')

    if source_type_index is None:
        sys.exit('SOURCE_TYPE_INDEX environment variable is required.')

    if manifest_inline and manifest_inline.strip():
        manifest_data = json.loads(manifest_inline)
        manifest_paths = _parse_manifest_data(manifest_data)
    else:
        if not manifest_file_raw:
            sys.exit('One of MANIFEST or MANIFEST_FILE environment variables is required.')
        manifest_file = Path(manifest_file_raw)
        if not manifest_file.exists():
            sys.exit(f'MANIFEST_FILE does not exist: {manifest_file}')

        with open(manifest_file, 'r', encoding='utf-8') as manifest_handle:
            manifest_data = json.load(manifest_handle)
        manifest_paths = _parse_manifest_data(manifest_data)

    if not manifest_paths:
        print('No valid paths found in MANIFEST input.')
        return

    storage_client = storage.Client()
    ingest_bucket = storage_client.bucket(ingest_bucket_name)

    def get_part(parts: list[str], index: int | None) -> str | None:
        if index is None:
            return None
        if index < 0 or index >= len(parts):
            return None
        return parts[index]

    def parse_blob_metadata(blob_name: str) -> tuple[str | None, str | None, str | None, str | None]:
        blob_pattern = re.compile(
            rf"^{re.escape(bucket_version_path)}/([^/]+)/ms=(\d{{4}})-(\d{{2}})/source_id=([^/]+)/"
        )
        match = blob_pattern.search(blob_name)
        if not match:
            return None, None, None, None
        return match.group(1), match.group(2), match.group(3), match.group(4)

    downloaded_blob_names = set()

    for manifest_path in manifest_paths:
        parts = [part for part in manifest_path.strip('/').split('/') if part]
        if not parts:
            continue

        source_type = get_part(parts, source_type_index)
        download_year = get_part(parts, year_index)
        download_month = get_part(parts, month_index)
        download_day = get_part(parts, day_index)
        manifest_source_id = get_part(parts, source_id_index)

        prefix = bucket_version_path
        if source_type:
            prefix = f"{prefix}/{source_type}"
        if download_year and download_month:
            prefix = f"{prefix}/ms={download_year}-{download_month}"

        prefixes = [prefix]
        if manifest_source_id:
            normalized_source_id = manifest_source_id.replace('source_id=', '')
            prefixes = [f"{prefix}/source_id={normalized_source_id}"]

        for list_prefix in prefixes:
            for blob in ingest_bucket.list_blobs(prefix=list_prefix):
                if blob.name in downloaded_blob_names:
                    continue

                file_path_bucket = os.path.splitext(blob.name)[0]
                file_name_bucket = re.split('/', file_path_bucket)[-1]

                file_date_match = re.search(r'[0-9]{4}-[0-1]{1}[0-9]{1}-[0-3]{1}[0-9]{1}', file_name_bucket)
                if not file_date_match:
                    continue

                file_date = file_date_match.group(0)
                file_date_parts = file_date.split('-')
                file_year = file_date_parts[0]
                file_month = file_date_parts[1]
                file_day = file_date_parts[2]

                if download_year and download_year != file_year:
                    continue
                if download_month and download_month != file_month:
                    continue
                if download_day and download_day != file_day:
                    continue

                blob_source_type, blob_year, blob_month, blob_source_id = parse_blob_metadata(blob.name)
                if blob_year and download_year and blob_year != download_year:
                    continue
                if blob_month and download_month and blob_month != download_month:
                    continue
                if blob_source_id and manifest_source_id and blob_source_id != manifest_source_id.replace('source_id=', ''):
                    continue

                if download_day:
                    trigger_date = datetime(int(download_year), int(download_month), int(download_day))
                    bucket_file_date = datetime(int(file_year), int(file_month), int(file_day))
                    if trigger_date != bucket_file_date:
                        continue

                resolved_source_type = source_type_out or source_type or blob_source_type
                resolved_source_id = blob_source_id or manifest_source_id
                if resolved_source_id is not None:
                    resolved_source_id = resolved_source_id.replace('source_id=', '')

                if not resolved_source_type or not resolved_source_id:
                    continue

                file_name = file_name_bucket + '.parquet'
                file_path = Path(
                    output_directory,
                    resolved_source_type,
                    file_year,
                    file_month,
                    file_day,
                    resolved_source_id,
                    'data',
                    file_name,
                )

                print('File path is:  ', file_path)
                file_path.parent.mkdir(parents=True, exist_ok=True)
                with open(file_path, 'wb') as l0_data_file:
                    l0_data_file.write(blob.download_as_bytes())

                downloaded_blob_names.add(blob.name)

if __name__ == '__main__':
    l0_gcs_loader()
