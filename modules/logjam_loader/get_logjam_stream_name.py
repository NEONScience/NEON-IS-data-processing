#!/usr/bin/env python3
import logging
from typing import Optional

from contextlib import closing


def get_logjam_stream_name(connection, asset_type: str, stream_number: int) -> Optional[str]:
    """
    Return the logjam stream name for an asset type and logjam stream number.

    :param connection: The database connection
    :param asset_type: The asset type
    :param stream_number: The logjam stream number
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
        and 
            is_ingest_term.stream_id = %(stream_number)s
        and 
            is_asset_definition.sensor_type_name = %(sensor_type_name)s
    '''
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, dict(sensor_type_name=asset_type, stream_number=stream_number))
        row = cursor.fetchone()
        if row is None:
            logging.error(f'Stream name not found for stream ID {stream_number} and asset type {asset_type}.')
            return None
        stream_name = row[0]
       # print(f'stream_name: {stream_name}')
    return stream_name
