from contextlib import closing

from geojson import Point, Feature, FeatureCollection

from lib.date_formatter import format


def get_all(connection, named_location_id):
    """
    Find location data for a named location ID.
    :param connection: A database connection.
    :param named_location_id: The ID for the named location to search.
    :return: Geojson FeatureCollection object containing location data.
    """
    with closing(connection.cursor()) as cursor:
        sql = '''
                select
                    locn_geom, 
                    locn_nam_locn_strt_date, 
                    locn_nam_locn_end_date, 
                    locn_nam_locn_tran_date, 
                    locn_alph_ortn, 
                    locn_beta_ortn, 
                    locn_gama_ortn, 
                    locn_x_off, 
                    locn_y_off, 
                    locn_z_off, 
                    nam_locn_id_off, 
                    nam_locn_name
                from locn
                join locn_nam_locn on locn.locn_id = locn_nam_locn.locn_id
                join nam_locn on locn.nam_locn_id_off = nam_locn.nam_locn_id
                where
                    locn_nam_locn.nam_locn_id = :named_location_id
            '''
        rows = cursor.execute(sql, named_location_id=named_location_id)
        locations = []
        for row in rows:
            location_geometry = row[0]
            start_date = row[1]
            end_date = row[2]
            transaction_date = row[3]
            alpha = row[4]
            beta = row[5]
            gamma = row[6]
            x_offset = row[7]
            y_offset = row[8]
            z_offset = row[9]
            named_location_id_offset = row[10]
            named_location_offset = row[11]

            geometry = None
            if location_geometry is not None:
                ordinates = location_geometry.SDO_ORDINATES
                if ordinates is not None:
                    ordinates = location_geometry.SDO_ORDINATES.aslist()
                    if len(ordinates) == 3:
                        geometry = Point((float(ordinates[0]), float(ordinates[1]), float(ordinates[2])))

            if (named_location_id_offset is not None) and (named_location_id_offset != named_location_id):
                reference_locations = get_all(connection, named_location_id_offset)
            else:
                reference_locations = None

            reference_location = Feature(geometry=None,
                                         properties={'name': named_location_offset, 'locations': reference_locations})

            location = Feature(geometry=geometry,
                               properties={'start_date': format(start_date),
                                           'end_date': format(end_date),
                                           'transaction_date': format(transaction_date),
                                           'alpha': alpha,
                                           'beta': beta,
                                           'gamma': gamma,
                                           'x_offset': x_offset,
                                           'y_offset': y_offset,
                                           'z_offset': z_offset,
                                           'reference_location': reference_location})
            locations.append(location)

        return FeatureCollection(locations)
