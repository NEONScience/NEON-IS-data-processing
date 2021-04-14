#!/usr/bin/env python3
from contextlib import closing
from typing import List

from geojson import Feature, FeatureCollection
from psycopg2 import extensions

from data_access.get_named_location_locations import get_named_location_locations
from data_access.get_named_location_context import get_named_location_context
from data_access.get_named_location_site import get_named_location_site
from data_access.get_named_location_properties import get_named_location_properties
from data_access.types.asset import Asset
from data_access.types.asset_location import AssetLocation
from data_access.types.property import Property
import data_access.types.geojson_converter as geojson_converter


def get_asset_locations(connection: extensions.connection, asset: Asset) -> FeatureCollection:
    """
    Get an asset's location history in GEOJson format.

    :param connection: The database connection.
    :param asset: The asset.
    :return: The asset's location history.
    """
    sql = '''
        select
            is_asset_location.nam_locn_id,
            is_asset_location.install_date,
            is_asset_location.remove_date,
            nam_locn.nam_locn_name,
            type.type_name
        from
            is_asset_location, nam_locn, type
        where
            is_asset_location.asset_uid = %s
        and
            nam_locn.nam_locn_id = is_asset_location.nam_locn_id
        and
            type.type_id = nam_locn.type_id
    '''
    features: List[Feature] = []
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [asset.id])
        rows = cursor.fetchall()
        for row in rows:
            key = row[0]
            install_date = row[1]
            remove_date = row[2]
            name = row[3]
            locations: FeatureCollection = get_named_location_locations(connection, key)
            properties: List[Property] = get_named_location_properties(connection, key)
            site: str = get_named_location_site(connection, key)
            context: List[str] = get_named_location_context(connection, key)
            asset_location = AssetLocation(name=name, site=site, install_date=install_date,
                                           remove_date=remove_date, context=context, properties=properties,
                                           locations=locations)
            features.append(geojson_converter.convert_asset_location(asset_location))
    feature_collection = FeatureCollection(features)
    # add the asset as the source
    feature_collection.update(source_id=asset.id)
    feature_collection.update(source_type=asset.type)
    return feature_collection
