#!/usr/bin/env python3
from contextlib import closing
from typing import List, Optional

from psycopg2 import extensions
from geojson import Point, Polygon, Feature, FeatureCollection

import common.date_formatter as date_formatter


def get_named_location_locations(connection: extensions.connection, named_location_id: int) -> FeatureCollection:
    """
    Get the locations associated with the named location in GEOJson format.

    :param connection: A database connection.
    :param named_location_id: The named location ID.
    :return: FeatureCollection of the locations associated with the named location.
    """
    sql = '''
        select
            ST_AsText(locn_geom) as point,
            locn_nam_locn_strt_date, 
            locn_nam_locn_end_date, 
            locn_alph_ortn, 
            locn_beta_ortn, 
            locn_gama_ortn, 
            locn_x_off, 
            locn_y_off, 
            locn_z_off, 
            nam_locn_id_off, 
            nam_locn_name
        from 
            locn
        join 
            locn_nam_locn on locn.locn_id = locn_nam_locn.locn_id
        join 
            nam_locn on locn.nam_locn_id_off = nam_locn.nam_locn_id
        where
            locn_nam_locn.nam_locn_id = %s
    '''
    features: List[Feature] = []
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [named_location_id])
        rows = cursor.fetchall()
        for row in rows:
            geometry = row[0]
            start_date = row[1]
            end_date = row[2]
            alpha = float(row[3])
            beta = float(row[4])
            gamma = float(row[5])
            x_offset = float(row[6])
            y_offset = float(row[7])
            z_offset = float(row[8])
            named_location_offset_id = row[9]
            named_location_offset_name = row[10]
            # convert dates
            if start_date is not None:
                start_date = date_formatter.to_string(start_date)
            if end_date is not None:
                end_date = date_formatter.to_string(end_date)
            # build the reference location
            reference_locations: Optional[FeatureCollection] = None
            if (named_location_offset_id is not None) and (named_location_offset_id != named_location_id):
                reference_locations = get_named_location_locations(connection, named_location_offset_id)
            reference_location_properties = dict(name=named_location_offset_name, locations=reference_locations)
            # build the location
            reference_feature = Feature(geometry=None, properties=reference_location_properties)
            properties = dict(start_date=start_date,
                              end_date=end_date,
                              alpha=alpha,
                              beta=beta,
                              gamma=gamma,
                              x_offset=x_offset,
                              y_offset=y_offset,
                              z_offset=z_offset,
                              reference_location=reference_feature)
            geojson_geometry = parse_geometry(geometry)
            feature = Feature(geometry=geojson_geometry, properties=properties)
            features.append(feature)
    return FeatureCollection(features)


def parse_geometry(geometry):
    if geometry is not None:
        ordinates: str = geometry[(geometry.find('(')+1):-1]
        if ordinates is not None:
            ordinate_list = list(ordinates.split(' '))
            if len(ordinate_list) == 3:
                x = float(ordinate_list[0])
                y = float(ordinate_list[1])
                z = float(ordinate_list[2])
                return Point((x, y, z))
            else:
                ordinates = ordinates.replace('(', '')
                ordinates = ordinates.replace(')', '')
                coordinate_list = ordinates.split(',')
                tuple_list = []
                for xyz in coordinate_list:
                    coordinates = xyz.split(' ')
                    x = float(coordinates[0])
                    y = float(coordinates[1])
                    z = float(coordinates[2])
                    tuple_list.append((x, y, z))
                return Polygon(coordinates=[*tuple_list])
    return None
