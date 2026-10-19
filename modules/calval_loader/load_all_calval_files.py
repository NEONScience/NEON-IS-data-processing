#!/usr/bin/env python3
from contextlib import closing
from pathlib import Path
import environs
import logging
import xml.etree.ElementTree as ET
import sys
import time
from calval_loader.get_avro_schema_name import get_avro_schema_name
from calval_loader.get_calibration_stream_name import get_calibration_stream_name
from google.cloud import storage
from data_access.db_config_reader import read_from_mount
from data_access.db_connector import DbConnector
import datetime


def load() -> None:
    env = environs.Env()
    log_level = env.str('LOG_LEVEL', 'INFO').upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format='%(asctime)s %(levelname)s %(name)s %(message)s',
    )
    logger = logging.getLogger(__name__)
    ingest_bucket_name = env.str('CVAL_INGEST_BUCKET')
    in_path: Path = env.path('IN_PATH')
    # print("IN_PATH value is:", in_path)
    output_directory: Path = env.path('OUT_PATH')
    trace_filename = env.str('TRACE_FILENAME', '')
    source_types = env.list('SOURCE_TYPES', default=[])
    if source_types:
        sensor_schemas = [(source_type, source_type) for source_type in source_types]
    else:
        sensor_type = env.str('SOURCE_TYPE')
        sensor_schemas = [(sensor_type, env.str('SCHEMA_NAME', sensor_type))]
    db_config = read_from_mount(Path('/var/db_secret'))
    storage_client = storage.Client()
    ingest_bucket = storage_client.bucket(ingest_bucket_name)
    starting_path_index: int = env.int('STARTING_PATH_INDEX')
    with closing(DbConnector(db_config)) as connector:
        now = datetime.datetime.now()
        current_filename = None
        trace_stage = 'input discovery'
        try:
            data_path_start = Path(*in_path.parts[0:starting_path_index + 1])  # starting index
            print("Starting New Datum in the load_al_calval_files pipeline ")
            for path in data_path_start.rglob('*'):
                if path.is_file():
                    print("Path value is: ", path)
                    filename = path.stem + ".xml"
                    current_filename = filename
                    tracing_file = filename == trace_filename
                    file_started = time.perf_counter()
                    if tracing_file:
                        logger.info('TRACE file=%s stage=input_found path=%s', filename, path)
                    print("FileName is: ", filename)
                    trace_stage = 'blob lookup'
                    stage_started = time.perf_counter()
                    if tracing_file:
                        logger.info('TRACE file=%s stage=blob_lookup_started bucket=%s', filename, ingest_bucket_name)
                    blob = ingest_bucket.get_blob(filename)
                    if tracing_file:
                        logger.info('TRACE file=%s stage=blob_lookup_completed elapsed_seconds=%.3f found=%s',
                                    filename, time.perf_counter() - stage_started, blob is not None)
                    if blob is None:
                        if tracing_file:
                            logger.warning('TRACE file=%s stage=skipped reason=blob_not_found', filename)
                        continue

                    trace_stage = 'blob download'
                    stage_started = time.perf_counter()
                    if tracing_file:
                        logger.info('TRACE file=%s stage=download_started', filename)
                    xml_content = blob.download_as_bytes()
                    if tracing_file:
                        logger.info('TRACE file=%s stage=download_completed bytes=%d elapsed_seconds=%.3f',
                                    filename, len(xml_content), time.perf_counter() - stage_started)

                    trace_stage = 'XML parsing'
                    root = ET.fromstring(xml_content)
                    asset_id = root.find('SensorID').find('MxAssetID').text
                    stream_id = root.find('StreamCalVal').find('StreamID').text
                    if tracing_file:
                        logger.info('TRACE file=%s stage=xml_parsed asset_id=%s stream_id=%s',
                                    filename, asset_id, stream_id)

                    trace_stage = 'schema lookup'
                    stage_started = time.perf_counter()
                    if tracing_file:
                        logger.info('TRACE file=%s stage=schema_lookup_started', filename)
                    avro_schema_name = get_avro_schema_name(connector.get_connection(), asset_id)
                    if tracing_file:
                        logger.info('TRACE file=%s stage=schema_lookup_completed schemas=%s elapsed_seconds=%.3f',
                                    filename, avro_schema_name, time.perf_counter() - stage_started)
                    if not avro_schema_name:
                        if tracing_file:
                            logger.warning('TRACE file=%s stage=skipped reason=no_schema asset_id=%s', filename, asset_id)
                        continue

                    matching_schemas = [
                        (sensor_type, schema_name)
                        for sensor_type, schema_name in sensor_schemas
                        if schema_name in avro_schema_name
                    ]
                    if matching_schemas:
                        if tracing_file:
                            logger.info('TRACE file=%s stage=schema_matched matches=%s', filename, matching_schemas)
                        for sensor_type, schema_name in matching_schemas:
                            trace_stage = 'stream lookup'
                            stage_started = time.perf_counter()
                            if tracing_file:
                                logger.info('TRACE file=%s stage=stream_lookup_started schema=%s stream_id=%s',
                                            filename, schema_name, stream_id)
                            stream_name = get_calibration_stream_name(connector.get_connection(), schema_name,
                                                                      stream_id)
                            if tracing_file:
                                logger.info('TRACE file=%s stage=stream_lookup_completed schema=%s stream_name=%s elapsed_seconds=%.3f',
                                            filename, schema_name, stream_name, time.perf_counter() - stage_started)
                            print('schema name , asset_id, stream_id, stream_name, filename are :', schema_name, "  ", asset_id,
                                  "  ", stream_id, " ", stream_name, " ", filename)
                            try:
                                output_path = Path(output_directory, sensor_type, asset_id, stream_name, filename)
                                output_path.parent.mkdir(parents=True, exist_ok=True)
                                trace_stage = 'output write'
                                with open(output_path, "wb") as output_file:
                                    bytes_written = output_file.write(xml_content)
                                if tracing_file:
                                    logger.info('TRACE file=%s stage=output_written path=%s bytes=%d elapsed_seconds=%.3f',
                                                filename, output_path, bytes_written, time.perf_counter() - file_started)
                            except Exception:
                                exc_type, exc_obj, exc_tb = sys.exc_info()
                                if tracing_file:
                                    logger.exception('TRACE file=%s stage=%s_failed', filename, trace_stage)
                                print("Exception at line " + str(exc_tb.tb_lineno) + ": " + str(sys.exc_info()))
                    elif tracing_file:
                        logger.warning('TRACE file=%s stage=skipped reason=no_matching_schema schemas=%s',
                                       filename, avro_schema_name)
        except Exception:
            exception_type, exception_obj, exception_tb = sys.exc_info()
            if current_filename == trace_filename:
                logger.exception('TRACE file=%s stage=%s_failed', current_filename, trace_stage)
            print("Exception at line " + str(exception_tb.tb_lineno) + ": " + str(sys.exc_info()))


if __name__ == '__main__':
    load()
