
"""Load L0 parquet files from GCS using manifest-driven record selectors.

Manifest input can be provided by either:
1. MANIFEST: JSON-formatted string.
2. MANIFEST_FILE: Path to a file containing JSON.

If both are set, MANIFEST is used.

Accepted JSON format: a JSON array of objects. Each object must contain the
keys "source_type" and "data_date". The optional "source_id" key narrows the
query to a specific source; when omitted it is wildcarded. Any additional keys
in a record are ignored.

The "data_date" value may be:
- "YYYY-mm-dd": filters to an exact day.
- "YYYY-mm": filters to a month (day wildcarded).
- "YYYY": filters to a year (month and day wildcarded).

Example manifest:
[
    {"source_type": "cmp22", "data_date": "2025-10-01", "source_id": "11185"},
    {"source_type": "cmp22", "data_date": "2025-10-02", "source_id": "11185"},
    {"source_type": "cmp22", "data_date": "2025-10"},
    {"source_type": "cmp22", "data_date": "2026"}
]

When source_id is present, the bucket prefix includes:
{L0_BUCKET_VERSION_PATH}/{source_type}/ms={download_year}-{download_month}/source_id={source_id}
"""

from google.cloud import storage
from pathlib import Path
import environs
import os
import sys
import re
import json
from datetime import datetime

import structlog
import common.log_config as log_config


def _parse_manifest_data(manifest_data: object, log) -> list[dict]:
    if not isinstance(manifest_data, list):
        log.error('Invalid manifest format', manifest_type=type(manifest_data).__name__)
        sys.exit('Manifest must be a JSON array of objects with "source_type" and "data_date" keys.')

    records = []
    for i, record in enumerate(manifest_data):
        if not isinstance(record, dict):
            log.warning('Skipping non-object manifest entry', index=i)
            continue
        if 'source_type' not in record:
            log.warning('Skipping manifest record missing required key "source_type"', index=i)
            continue
        if 'data_date' not in record:
            log.warning('Skipping manifest record missing required key "data_date"', index=i)
            continue
        records.append(record)
    return records


def l0_gcs_loader_by_manifest() -> None:

    env = environs.Env()
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    log_config.configure(log_level)
    log = structlog.get_logger()
    
    ingest_bucket_name = env.str('L0_BUCKET_NAME')
    bucket_version_path = env.str('L0_BUCKET_VERSION_PATH')
    source_type_out = env.str('SOURCE_TYPE_OUT', None)
    manifest_inline = env.str('MANIFEST', None)
    manifest_file_raw = env.str('MANIFEST_FILE', None)
    output_directory: Path = env.path('OUT_PATH')

    log.debug('Configuration loaded',
              bucket_name=ingest_bucket_name,
              output_directory=output_directory)

    if manifest_inline and manifest_inline.strip():
        try:
            manifest_data = json.loads(manifest_inline)
            log.debug('Manifest loaded from MANIFEST environment variable')
        except json.JSONDecodeError as exc:
            log.error('Invalid JSON in MANIFEST', error=str(exc))
            sys.exit(f'Invalid JSON in MANIFEST: {exc}')
        manifest_records = _parse_manifest_data(manifest_data, log)
    else:
        if not manifest_file_raw:
            log.error('One of MANIFEST or MANIFEST_FILE environment variables is required')
            sys.exit('One of MANIFEST or MANIFEST_FILE environment variables is required.')
        manifest_file = Path(manifest_file_raw)
        if not manifest_file.exists():
            log.error('MANIFEST_FILE does not exist', manifest_file=str(manifest_file))
            sys.exit(f'MANIFEST_FILE does not exist: {manifest_file}')

        log.debug('Loading manifest from file', manifest_file=str(manifest_file))
        with open(manifest_file, 'r', encoding='utf-8') as manifest_handle:
            try:
                manifest_data = json.load(manifest_handle)
                log.debug('Manifest loaded from file', manifest_file=str(manifest_file))
            except json.JSONDecodeError as exc:
                log.error('Invalid JSON in MANIFEST_FILE', manifest_file=str(manifest_file), error=str(exc))
                sys.exit(f'Invalid JSON in MANIFEST_FILE {manifest_file}: {exc}')
        manifest_records = _parse_manifest_data(manifest_data, log)

    if not manifest_records:
        log.warning('No valid records found in MANIFEST input')
        return

    log.info('Processing manifest records', record_count=len(manifest_records))

    storage_client = storage.Client()
    ingest_bucket = storage_client.bucket(ingest_bucket_name)
    log.debug('Connected to GCS bucket', bucket_name=ingest_bucket_name)

    def parse_blob_metadata(blob_name: str) -> tuple[str | None, str | None, str | None, str | None]:
        blob_pattern = re.compile(
            rf"^{re.escape(bucket_version_path)}/([^/]+)/ms=(\d{{4}})-(\d{{2}})/source_id=([^/]+)/"
        )
        match = blob_pattern.search(blob_name)
        if not match:
            return None, None, None, None
        return match.group(1), match.group(2), match.group(3), match.group(4)

    downloaded_blob_names = set()

    for record in manifest_records:
        source_type = record['source_type']
        data_date = record['data_date']
        manifest_source_id = record.get('source_id', None)

        log.debug('Processing manifest record', source_type=source_type, data_date=data_date, source_id=manifest_source_id)

        date_parts = data_date.split('-')
        download_year = date_parts[0] if len(date_parts) >= 1 else None
        download_month = date_parts[1] if len(date_parts) >= 2 else None
        download_day = date_parts[2] if len(date_parts) >= 3 else None

        prefix = bucket_version_path
        if source_type:
            prefix = f"{prefix}/{source_type}"
        if download_year and download_month:
            prefix = f"{prefix}/ms={download_year}-{download_month}"

        prefixes = [prefix]
        if manifest_source_id:
            normalized_source_id = manifest_source_id.replace('source_id=', '')
            prefixes = [f"{prefix}/source_id={normalized_source_id}"]

        files_downloaded_for_path = 0
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
                    if not download_year or not download_month:
                        continue
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

                log.debug('Downloading file to local path', 
                         file_path=str(file_path),
                         blob_name=blob.name,
                         resolved_source_type=resolved_source_type,
                         resolved_source_id=resolved_source_id,
                         file_date=file_date)
                file_path.parent.mkdir(parents=True, exist_ok=True)
                with open(file_path, 'wb') as l0_data_file:
                    l0_data_file.write(blob.download_as_bytes())
                log.info('File downloaded successfully', 
                        file_path=str(file_path),
                        blob_name=blob.name)

                downloaded_blob_names.add(blob.name)
                files_downloaded_for_path += 1

        if files_downloaded_for_path == 0:
            log.warning('No files found in bucket for manifest record',
                        source_type=source_type,
                        data_date=data_date,
                        source_id=manifest_source_id,
                        bucket_prefix=prefixes[0] if prefixes else 'N/A')

    log.info('Manifest processing completed', total_files_downloaded=len(downloaded_blob_names))

if __name__ == '__main__':
    l0_gcs_loader_by_manifest()
