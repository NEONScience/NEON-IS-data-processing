#!/usr/bin/env python3
import logging
from typing import Optional

from contextlib import closing


def get_calibration_stream_name(connection, schema_name: str, stream_number: int) -> Optional[str]:
    """
    Return the calibration stream name for an asset type and calibration stream number.

    :param connection: The database connection
    :param schema_name: The schema name of the data
    :param stream_number: The calibration stream number
    :return: The stream name
    """
    sql = '''
        select distinct
            is_ingest_term.schema_field_name
        from
            is_ingest_term
        join
            is_asset_definition
        on
            is_asset_definition.asset_definition_uuid = is_ingest_term.asset_definition_uuid
        join is_sensor_type ist
        on is_asset_definition.sensor_type_name = ist.sensor_type_name
        and
            is_ingest_term.stream_id = %(stream_number)s
        and
            ist.avro_schema_name = %(avro_schema_name)s
    '''
    # print(f'Finding stream name for schema_name: {schema_name} and stream_number: {stream_number}')
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, dict(avro_schema_name=schema_name, stream_number=stream_number))
        row = cursor.fetchone()
        if row is None:
            logging.error(f'Stream name not found for stream ID {stream_number} and asset type {schema_name}.')
            return None
        stream_name = row[0]
        # print(f'asset_type: {schema_name}    stream_name: {stream_name}')
    return stream_name
