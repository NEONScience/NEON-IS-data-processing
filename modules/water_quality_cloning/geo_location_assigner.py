import datetime

from contextlib import closing


def get_locations(connection, named_location_id):
    """
    Get all the geo-locations assigned over time to a particular named location.
    :param connection: A database connection.
    :param named_location_id: The named location ID.
    :return: A List of geo-locations IDs with start and end dates.
    """
    sql = '''
        select locn_nam_locn_strt_date, locn_nam_locn_end_date, locn_id from locn_nam_locn where nam_locn_id = :id
    '''
    with closing(connection.cursor()) as cursor:
        cursor.prepare(sql)
        rows = cursor.execute(None, id=named_location_id)
        locations = []
        for row in rows:
            start_date = row[0]
            end_date = row[1]
            location_id = row[2]
            locations.append({'location_id': location_id, 'start_date': start_date, 'end_date': end_date})
    return locations


def assign_locations(connection, named_location_id, clone_id):
    """
    Get the geo-location history for an existing named location and assign this history
    to the given cloned named location.
    :param connection: A database connection.
    :param named_location_id: The existing named location used as the source location.
    :param clone_id: A new named location ID to assign to the location history of the existing named location ID.
    :return:
    """
    sql = '''
        insert into locn_nam_locn (
            locn_nam_locn_id, 
            locn_id, 
            nam_locn_id, 
            locn_nam_locn_strt_date, 
            locn_nam_locn_end_date, 
            locn_nam_locn_tran_date
        ) 
        values (
            locn_nam_locn_id_seq1.nextval,
            :location_id,
            :named_location_id,
            :start_date,
            :end_date,
            :tran_date
        )
    '''
    # Get the geo-location history for the existing named location.
    locations = get_locations(connection, named_location_id)
    # Loop over the geo-locations and create associations with the cloned named locations.
    with closing(connection.cursor()) as cursor:
        cursor.prepare(sql)
        for location in locations:
            location_id = location.get('location_id')
            start_date = location.get('start_date')
            end_date = location.get('end_date')
            print(f'assigning locations for named location clone: {clone_id}')
            cursor.execute(None,
                           location_id=location_id,
                           named_location_id=clone_id,
                           start_date=start_date,
                           end_date=end_date,
                           tran_date=datetime.datetime.now())
        connection.commit()
