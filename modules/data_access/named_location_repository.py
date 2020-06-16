#!/usr/bin/env python3
from contextlib import closing

from geojson import Feature, FeatureCollection
import structlog

from data_access.active_period_repository import ActivePeriodRepository
from data_access.location_repository import LocationRepository
from data_access.named_location_context_repository import NamedLocationContextRepository
from data_access.named_location_parent_repository import NamedLocationParentRepository
from data_access.property_repository import PropertyRepository

import common.date_formatter as date_formatter

log = structlog.get_logger()


class NamedLocationRepository(object):
    """Class to represent a named location repository backed by a database."""

    def __init__(self, connection):
        self.connection = connection
        self.active_period_repository = ActivePeriodRepository(connection)
        self.location_repository = LocationRepository(connection)
        self.context_repository = NamedLocationContextRepository(connection)
        self.parent_repository = NamedLocationParentRepository(connection)
        self.property_repository = PropertyRepository(connection)

    def get_by_type(self, location_type: str, cutoff_date=None):
        """
        Get named locations in GEOJson format by type and cutoff date for the active time range.

        :param location_type: The named location type.
        :param cutoff_date: The maximum active end time to return.
        :type cutoff_date: datetime object
        :return: Geojson FeatureCollection of locations.
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
                features = []
                named_location_id = row[0]
                name = row[1]
                description = row[2]
                log.debug(f'named_location name: {name} description {description}')
                properties = self.property_repository.get_named_location_properties(named_location_id)
                parents = self.parent_repository.get_parents(named_location_id)
                context = self.context_repository.get_context(named_location_id)
                active_periods = \
                    self.active_period_repository.get_active_periods(named_location_id, cutoff_date=cutoff_date)
                # The parent value contains only site
                site = parents[0]['name']
                feature = Feature(properties={'name': name,
                                              'type': location_type,
                                              'description': description,
                                              'site': site,
                                              'context': context,
                                              'active_periods': active_periods})
                for k, v in properties.items():
                    feature.update({k: v})
                features.append(feature)
                yield FeatureCollection(features)

    def get_asset_location_history(self, asset_id: int):
        """
        Get an asset's location history.

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
            features = []
            for row in rows:
                named_location_id = row[0]
                install_date = row[1]
                remove_date = row[2]
                transaction_date = row[3]
                named_location_name = row[4]

                if install_date is not None:
                    install_date = date_formatter.convert(install_date)
                if remove_date is not None:
                    remove_date = date_formatter.convert(remove_date)
                if transaction_date is not None:
                    transaction_date = date_formatter.convert(transaction_date)

                locations = self.location_repository.get_all(named_location_id)
                properties = self.property_repository.get_named_location_properties(named_location_id)
                parents = self.parent_repository.get_parents(named_location_id)
                context = self.context_repository.get_context(named_location_id)

                # The parent value only contains site
                site = parents[0]['name']
                feature = Feature(properties={'name': named_location_name,
                                              'site': site,
                                              'install_date': install_date,
                                              'remove_date': remove_date,
                                              'transaction_date': transaction_date,
                                              'context': context,
                                              'locations': locations})
                for k, v in properties.items():
                    feature.update({k: v})
                features.append(feature)
        return FeatureCollection(features)

    def get_site(self, named_location_id: int):
        """
        Get the site for a named location.

        :param named_location_id: A named location ID.
        :return: The site name.
        """
        site_name = None
        parents = self.parent_repository.get_parents(named_location_id)
        for parent in parents:
            if parent.get('type').lower() == 'site':
                site_name = parent.get('name')
        return site_name

    def get_schema_name(self, named_location_name: str):
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
