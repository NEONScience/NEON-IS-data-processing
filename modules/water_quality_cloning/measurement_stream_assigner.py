#!/usr/bin/env python3
from contextlib import closing


def get_streams(connection, named_location_id):
    """
    Return the measurement streams associated with a named location ID.

    :param connection: A database connection.
    :type connection: connection object.
    :param named_location_id: The named location ID for finding rows.
    :type named_location_id: int
    :return: A list of streams.
    """
    sql = '''
        select 
            meas_strm.meas_strm_id, 
            meas_strm.meas_strm_name, 
            meas_strm.nam_locn_id, 
            meas_strm.term_name, 
            is_ingest_term.schema_field_name
        from 
            meas_strm, is_ingest_term
        where 
            is_ingest_term.term_name = meas_strm.term_name
        and
            meas_strm.nam_locn_id = :id
    '''
    streams = []
    with closing(connection.cursor()) as cursor:
        cursor.prepare(sql)
        rows = cursor.execute(None, id=named_location_id)
        for row in rows:
            stream_id = row[0]
            name = row[1]
            named_location = row[2]
            term = row[3]
            field_name = row[4]
            stream = {'stream_id': stream_id,
                      'name': name,
                      'named_location_id': named_location,
                      'term': term,
                      'field': field_name}
            streams.append(stream)
    return streams


def reassign_stream(connection, measurement_stream_id, cloned_name_location_id):
    """
    Reassign the measurement streams to the cloned named location ID.

    :param connection: A database connection.
    :type connection: connection object
    :param measurement_stream_id: The measurement stream ID.
    :type measurement_stream_id: int
    :param cloned_name_location_id: The cloned named location ID.
    :type cloned_name_location_id: int
    :return:
    """
    sql = '''
        update meas_strm set nam_locn_id = :named_location_id where meas_strm_id = :measurement_stream_id
    '''
    with closing(connection.cursor()) as cursor:
        cursor.prepare(sql)
        print(f'Assigning stream {measurement_stream_id} to named location clone {cloned_name_location_id}')
        cursor.execute(None, named_location_id=cloned_name_location_id, measurement_stream_id=measurement_stream_id)
        connection.commit()
