#!/usr/bin/env python3
from contextlib import closing
from typing import List, Optional

from cx_Oracle import Connection
from geojson import Point, Feature, FeatureCollection

import common.date_formatter as date_formatter


class LocationRepository(object):
    """Class to represent a location repository backed by a database."""

    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    def get_locations(self, named_location_id: int) -> FeatureCollection:
        """
        Get the locations associated with the named location in GEOJson format.

        :param named_location_id: The named location ID.
        :return: FeatureCollection of the locations associated with the named location.
        """
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
        with closing(self.connection.cursor()) as cursor:
            rows = cursor.execute(sql, named_location_id=named_location_id)
            locations: List[Feature] = []
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
                named_location_offset_id = row[10]
                named_location_offset_name = row[11]

                if start_date is not None:
                    start_date = date_formatter.convert(start_date)
                if end_date is not None:
                    end_date = date_formatter.convert(end_date)
                if transaction_date is not None:
                    transaction_date = date_formatter.convert(transaction_date)

                # get the reference locations for this location
                reference_locations = None
                if (named_location_offset_id is not None) and (named_location_offset_id != named_location_id):
                    reference_locations = self.get_locations(named_location_offset_id)
                reference_location = Feature(geometry=None,
                                             properties={'name': named_location_offset_name,
                                                         'locations': reference_locations})
                # build the location
                point = self.get_point(location_geometry)
                location = Feature(geometry=point,
                                   properties={'start_date': start_date,
                                               'end_date': end_date,
                                               'transaction_date': transaction_date,
                                               'alpha': alpha,
                                               'beta': beta,
                                               'gamma': gamma,
                                               'x_offset': x_offset,
                                               'y_offset': y_offset,
                                               'z_offset': z_offset,
                                               'reference_location': reference_location})
                locations.append(location)
            return FeatureCollection(locations)

    @staticmethod
    def get_point(location_geometry) -> Optional[Point]:
        if location_geometry is not None:
            ordinates = location_geometry.SDO_ORDINATES
            if ordinates is not None:
                ordinates = location_geometry.SDO_ORDINATES.aslist()
                if len(ordinates) == 3:
                    point = Point((float(ordinates[0]), float(ordinates[1]), float(ordinates[2])))
                    return point
        return None
