#!/usr/bin/env python3
from contextlib import closing
from typing import List, Iterator, Optional

from geojson import Feature, FeatureCollection
from cx_Oracle import Connection

from data_access.active_period_repository import ActivePeriodRepository
from data_access.location_repository import LocationRepository
from data_access.named_location_context_repository import NamedLocationContextRepository
from data_access.named_location_parent_repository import NamedLocationParentRepository
from data_access.property_repository import PropertyRepository
from data_access.active_period import ActivePeriod
from data_access.asset_location import AssetLocation
from data_access.named_location import NamedLocation
from data_access.property import Property
import data_access.geojson_converter as geojson_converter


class NamedLocationRepository(object):
    """Class to represent a named location repository backed by a database."""

    def __init__(self, connection: Connection) -> None:
        self.connection = connection
        self.active_period_repository = ActivePeriodRepository(connection)
        self.location_repository = LocationRepository(connection)
        self.context_repository = NamedLocationContextRepository(connection)
        self.parent_repository = NamedLocationParentRepository(connection)
        self.property_repository = PropertyRepository(connection)

    def get_named_locations(self, location_type: str) -> Iterator[FeatureCollection]:
        """
        Get named locations in GEOJson format.

        :param location_type: The named location type.
        :return: Geojson Feature of named location data.
        """
        sql = '''
            select
                nam_locn.nam_locn_id,
                nam_locn.nam_locn_name,
                nam_locn.nam_locn_desc,
                type.type_name
            from
                nam_locn, type, nam_locn_context
            where
                type.type_id = nam_locn.type_id
            and
                type.type_name = :location_type
            and 
                nam_locn.nam_locn_id = nam_locn_context.nam_locn_id 
        '''
        with closing(self.connection.cursor()) as cursor:
            cursor.prepare(sql)
            rows = cursor.execute(None, location_type=location_type)
            for row in rows:
                key = row[0]
                name = row[1]
                description = row[2]
                properties: List[Property] = self.property_repository.get_named_location_properties(key)
                site: str = self.parent_repository.get_site(key)
                context: List[str] = self.context_repository.get_context(key)
                active_periods: List[ActivePeriod] = self.active_period_repository.get_active_periods(key)
                named_location = NamedLocation(name=name, type=location_type, description=description,
                                               site=site, context=context, active_periods=active_periods,
                                               properties=properties)
                yield geojson_converter.convert_named_location(named_location)

    def get_asset_location_history(self, asset_id: int) -> FeatureCollection:
        """
        Get an asset's location history in GEOJson format.

        :param asset_id: The asset ID.
        :return: The asset's location history.
        """
        sql = '''
            select
                is_asset_location.nam_locn_id,
                is_asset_location.install_date,
                is_asset_location.remove_date,
                is_asset_location.tran_date,
                nam_locn.nam_locn_name,
                type.type_name
            from
                is_asset_location, nam_locn, type
            where
                is_asset_location.asset_uid = :asset_id
            and
                nam_locn.nam_locn_id = is_asset_location.nam_locn_id
            and
                type.type_id = nam_locn.type_id
        '''
        with closing(self.connection.cursor()) as cursor:
            cursor.prepare(sql)
            rows = cursor.execute(None, asset_id=asset_id)
            features: List[Feature] = []
            for row in rows:
                key = row[0]
                install_date = row[1]
                remove_date = row[2]
                transaction_date = row[3]
                name = row[4]
                locations: FeatureCollection = self.location_repository.get_locations(key)
                properties: List[Property] = self.property_repository.get_named_location_properties(key)
                site: str = self.parent_repository.get_site(key)
                context: List[str] = self.context_repository.get_context(key)
                asset_location = AssetLocation(name=name, site=site, install_date=install_date,
                                               remove_date=remove_date, transaction_date=transaction_date,
                                               context=context, properties=properties, locations=locations)
                features.append(geojson_converter.convert_asset_location(asset_location))
        return FeatureCollection(features)

    def get_schema_name(self, named_location_name: str) -> Optional[str]:
        """
        Return the schema name for the named location's required sensor type.

        :param named_location_name: The named location name.
        :return: The schema name.
        """
        sql = '''
            select distinct 
                avro_schema_name
            from 
                is_sensor_type, is_asset_definition, is_asset_assignment, is_asset_location, nam_locn
            where
                is_sensor_type.sensor_type_name = is_asset_definition.sensor_type_name
            and 
                is_asset_definition.asset_definition_uuid = is_asset_assignment.asset_definition_uuid
            and 
                is_asset_assignment.asset_uid = is_asset_location.asset_uid
            and 
                is_asset_location.nam_locn_id = nam_locn.nam_locn_id
            and 
                nam_locn.nam_locn_name = :name
        '''
        schema_name = None
        with closing(self.connection.cursor()) as cursor:
            cursor.prepare(sql)
            cursor.execute(None, name=named_location_name)
            row = cursor.fetchone()
            if row is not None:
                schema_name = row[0]
        return schema_name
