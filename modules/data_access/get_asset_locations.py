#!/usr/bin/env python3
from contextlib import closing
from typing import List, Tuple, Set

from geojson import Feature, FeatureCollection

from data_access.get_assets import get_asset_definition_by_date
from data_access.get_named_location_geolocations import get_named_location_geolocations
from data_access.get_named_location_context import get_named_location_context
from data_access.get_named_location_parents import get_named_location_parents
from data_access.get_named_location_properties import get_named_location_properties
from data_access.db_connector import DbConnector
from data_access.types.asset import Asset
from data_access.types.asset_location import AssetLocation
from data_access.types.property import Property
import data_access.types.geojson_converter as geojson_converter


def get_asset_locations(connector: DbConnector, asset: Asset) -> FeatureCollection:
    """
    Get an asset's location history in GEOJson format.

    :param connector: The database connection.
    :param asset: The asset.
    :return: The asset's location history.
    """
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'''
        select
            is_asset_location.nam_locn_id,
            is_asset_location.install_date,
            is_asset_location.remove_date,
            nam_locn.nam_locn_name,
            type.type_name
        from
            {schema}.is_asset_location, {schema}.nam_locn, {schema}.type
        where
            is_asset_location.asset_uid = %s
        and
            nam_locn.nam_locn_id = is_asset_location.nam_locn_id
        and
            type.type_id = nam_locn.type_id
        order by 
            is_asset_location.install_date;
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
            locations: FeatureCollection = get_named_location_geolocations(connector, key)
            properties: List[Property] = get_named_location_properties(connector, key)
            # get asset information (model, manufacturer, software version) and append to properties
            all_asset: Set[Asset] = get_asset_definition_by_date(connector, install_date, remove_date, asset.id)
            asset_def = next(iter(all_asset), None)
            if asset_def:
                properties.extend([Property(name="asset_model", value=asset_def.model),
                                   Property(name="asset_manufacturer", value=asset_def.manufacturer),
                                   Property(name="asset_software_version", value=asset_def.software_version)])

            parents: dict[str, Tuple[int, str]] = get_named_location_parents(connector, key)
            (domain_id, domain) = parents['domain'] if parents else None
            (site_id, site) = parents['site'] if parents else None
            site_location: FeatureCollection = get_named_location_geolocations(connector, site_id)
            context: List[str] = get_named_location_context(connector, key)
            asset_location = AssetLocation(name=name, domain=domain, site=site, install_date=install_date,
                                           remove_date=remove_date, context=context, properties=properties,
                                           site_location=site_location, locations=locations)
            features.append(geojson_converter.convert_asset_location(asset_location))
    feature_collection = FeatureCollection(features)
    # add the asset as the source
    feature_collection.update(source_id=asset.id)
    feature_collection.update(source_type=asset.type)
    return feature_collection
