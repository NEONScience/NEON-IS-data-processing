#!/usr/bin/env python3
from contextlib import closing
from typing import List, Optional

from psycopg2 import extensions, extras
from geojson import Point, Feature, FeatureCollection

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
            locn_nam_locn.nam_locn_id = 3
    '''
    features: List[Feature] = []
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [named_location_id])
        rows = cursor.fetchall()
        for row in rows:
            geometry = row[0]
            start_date = row[1]
            end_date = row[2]
            alpha = row[3]
            beta = row[4]
            gamma = row[5]
            x_offset = row[6]
            y_offset = row[7]
            z_offset = row[8]
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
            point = get_point(geometry)
            feature = Feature(geometry=point, properties=properties)
            features.append(feature)
    return FeatureCollection(features)


def get_ordinates(geometry) -> Optional[List]:
    if geometry is not None:
        ordinates = geometry.SDO_ORDINATES
        if ordinates is not None:
            ordinate_list = ordinates.aslist()
            if len(ordinate_list) == 3:
                return ordinate_list
    return None


def get_point(geometry) -> Optional[Point]:
    ordinates = get_ordinates(geometry)
    if ordinates is not None:
        x = float(ordinates[0])
        y = float(ordinates[1])
        z = float(ordinates[2])
        return Point((x, y, z))
    return None
