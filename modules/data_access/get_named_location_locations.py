#!/usr/bin/env python3
from contextlib import closing
from typing import List, Optional

from cx_Oracle import Connection
from geojson import Point, Polygon, Feature, FeatureCollection

import common.date_formatter as date_formatter
from data_access.get_geolocation_properties import get_geolocation_properties


def get_named_location_locations(connection: Connection, named_location_id: int) -> FeatureCollection:
    """
    Get the locations associated with the named location in GEOJson format.

    :param connection: A database connection.
    :param named_location_id: The named location ID.
    :return: FeatureCollection of the locations associated with the named location.
    """
    sql = '''
        select
            locn.locn_id,
            locn.locn_geom, 
            locn_nam_locn.locn_nam_locn_strt_date, 
            locn_nam_locn.locn_nam_locn_end_date, 
            locn.locn_alph_ortn, 
            locn.locn_beta_ortn, 
            locn.locn_gama_ortn, 
            locn.locn_x_off, 
            locn.locn_y_off, 
            locn.locn_z_off, 
            locn.nam_locn_id_off, 
            nam_locn.nam_locn_name
        from 
            locn
        join 
            locn_nam_locn on locn.locn_id = locn_nam_locn.locn_id
        join 
            nam_locn on locn.nam_locn_id_off = nam_locn.nam_locn_id
        where
            locn_nam_locn.nam_locn_id = :named_location_id
    '''
    features: List[Feature] = []
    with closing(connection.cursor()) as cursor:
        rows = cursor.execute(sql, named_location_id=named_location_id)
        for row in rows:
            location_id = row[0]
            geometry = row[1]
            start_date = row[2]
            end_date = row[3]
            alpha = row[4]
            beta = row[5]
            gamma = row[6]
            x_offset = row[7]
            y_offset = row[8]
            z_offset = row[9]
            named_location_offset_id = row[10]
            named_location_offset_name = row[11]
            location_properties = get_geolocation_properties(connection, location_id)
            # convert dates
            if start_date is not None:
                start_date = date_formatter.to_string(start_date)
            if end_date is not None:
                end_date = date_formatter.to_string(end_date)
            # retrieve the reference locations
            # reference_locations = None
            reference_feature = None
            if (named_location_offset_id is not None) and (named_location_offset_id != named_location_id):
                # recursively retrieve the reference locations
                reference_locations = get_named_location_locations(connection, named_location_offset_id)
                # build the reference feature
                reference_location_properties = dict(name=named_location_offset_name, locations=reference_locations)
                reference_feature = Feature(geometry=None, properties=reference_location_properties)
            properties = dict(start_date=start_date,
                              end_date=end_date,
                              alpha=alpha,
                              beta=beta,
                              gamma=gamma,
                              x_offset=x_offset,
                              y_offset=y_offset,
                              z_offset=z_offset,
                              reference_location=reference_feature,
                              location_properties=location_properties)
            geojson_geometry = parse_geometry(geometry)
            feature = Feature(geometry=geojson_geometry, properties=properties)
            features.append(feature)
    return FeatureCollection(features)


def parse_geometry(geometry):
    if geometry is not None:
        ordinates = geometry.SDO_ORDINATES
        if ordinates is not None:
            ordinate_list = ordinates.aslist()
            if len(ordinate_list) == 3:
                x = float(ordinate_list[0])
                y = float(ordinate_list[1])
                z = float(ordinate_list[2])
                return Point((x, y, z))
            else:
                return Polygon(zip(*[iter(ordinate_list)] * 3))
    return None
