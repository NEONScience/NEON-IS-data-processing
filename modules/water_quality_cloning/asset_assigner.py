#!/usr/bin/env python3
import datetime
from typing import List

from contextlib import closing
from cx_Oracle import Connection

from water_quality_cloning.measurement_stream_assigner import MeasurementStreamAssigner


class AssetAssigner(object):

    def __init__(self, connection: Connection, measurement_stream_assigner: MeasurementStreamAssigner) -> None:
        self.connection = connection
        self.measurement_stream_assigner = measurement_stream_assigner

    def get_assets_at_location(self, named_location_id: int) -> List[dict]:
        """
        Get all the assets currently assigned to a PRT location.

        :param named_location_id: A named location ID.
        :return: A list of assets currently assigned to the named location at the given ID.
        """
        sql = '''
            select
                is_asset_location.asset_location_id,
                is_asset_location.asset_uid,
                is_asset_location.install_date,
                is_asset_location.remove_date,
                is_asset_definition.sensor_type_name
            from
                is_asset_location, is_asset_definition, is_asset_assignment
            where
                is_asset_assignment.asset_uid = is_asset_location.asset_uid
            and
                is_asset_definition.asset_definition_uuid = is_asset_assignment.asset_definition_uuid
            and 
                is_asset_definition.sensor_type_name not like 'prt'
            and 
                is_asset_definition.sensor_type_name not like 'TO DO'
            and
                is_asset_location.nam_locn_id = :id
        '''
        assets = []
        with closing(self.connection.cursor()) as cursor:
            rows = cursor.execute(sql, id=named_location_id)
            for row in rows:
                asset_location_id = row[0]
                asset_uid = row[1]
                install_date = row[2]
                remove_date = row[3]
                sensor_type = row[4]
                assets.append({
                    'asset_location_id': asset_location_id,
                    'asset_uid': asset_uid,
                    'install_date': install_date,
                    'remove_date': remove_date,
                    'sensor_type': sensor_type})
        return assets

    def assign_assets(self, named_location_id: int, cloned_locations: list) -> None:
        """
        Reassign assets from the PRT named location to the cloned named location based on the asset (sensor) type.

        :param named_location_id: A named location ID.
        :param cloned_locations: The new named locations for assigning assets.
        """
        print(f'assigning assets.')
        sql = '''
            update 
                is_asset_location
            set 
                nam_locn_id = :named_location_id, 
                change_by = :change_by, 
                tran_date = :tran_date
            where 
                asset_location_id = :asset_location_id
        '''
        assets = self.get_assets_at_location(named_location_id)
        print(f'Found {len(assets)} assets.')
        for asset in assets:
            asset_location_id = asset.get('asset_location_id')
            sensor_type = asset.get('sensor_type')
            if sensor_type != 'prt':
                sensor_type_index = self.get_clone_location_index(sensor_type)
                print(f'sensor type: {sensor_type}, sensor type index: {sensor_type_index}, '
                      f'cloned locations: {len(cloned_locations)}')
                cloned_location = cloned_locations[sensor_type_index]
                cloned_location_id = cloned_location.get('key')
                with closing(self.connection.cursor()) as cursor:
                    cursor.execute(sql,
                                   named_location_id=cloned_location_id,
                                   change_by='water quality prototype',
                                   tran_date=datetime.datetime.now(),
                                   asset_location_id=asset_location_id)
                    self.connection.commit()
                # The location is now assigned to a sensor type, assign any measurement streams of the same
                # type currently assigned to the old location to the new cloned location.
                self.assign_measurement_streams(named_location_id, cloned_location_id, sensor_type)

    @staticmethod
    def get_clone_location_index(sensor_type: str) -> int:
        """
        Ensure all sensors of the same type are assigned to the same cloned named location.

        :param sensor_type: A type of sensor.
        :return: The index number for the sensor type.
        """
        switcher = {
            'exo2': 0,
            'exoconductivity': 1,
            'exodissolvedoxygen': 2,
            'exoturbidity': 3,
            'exophorp': 4,
            'exototalalgae': 5,
            'exofdom': 6
        }
        index = switcher.get(sensor_type, "Invalid sensor type.")
        return index

    @staticmethod
    def get_field_names(sensor_type: str) -> List[str]:
        """
        Get all the schema field names for a sensor type.

        :param sensor_type: A type of sensor.
        :return: The field names for the sensor type.
        """
        switcher = {
            'exo2': ['sensorDepth', 'sondeSurfaceWaterPressure', 'wiperPosition', 'batteryVoltage', 'sensorVoltage'],
            'exoconductivity': ['conductance', 'specificConductance', 'surfaceWaterTemperature'],
            'exodissolvedoxygen': ['dissolvedOxygenSaturation', 'dissolvedOxygen'],
            'exoturbidity': ['turbidityRaw', 'turbidity'],
            'exophorp': ['pH', 'pHvoltage'],
            'exototalalgae': ['blueGreenAlgaeRaw', 'blueGreenAlgaePhycocyanin', 'chlorophyllRaw', 'chlorophyll'],
            'exofdom': ['fDOMRaw', 'fDOM']
        }
        index = switcher.get(sensor_type, "Invalid sensor type.")
        return index

    def assign_measurement_streams(self, named_location_id: int, cloned_location_id: int, sensor_type: str) -> None:
        """
        Reassign a measurement stream from the named_location_id to the cloned_location_id if the
        measurement stream schema field name (associated by the term name) matches the cloned
        named location sensor type.

        :param named_location_id: The named location ID.
        :param cloned_location_id: The cloned location ID.
        :param sensor_type: The sensor type.
        """
        print(f'assigning measurement streams')
        streams = self.measurement_stream_assigner.get_streams(named_location_id)
        for stream in streams:
            if sensor_type != 'prt':
                sensor_field_names = self.get_field_names(sensor_type)
                stream_field_name = stream.get('field')
                print(f'stream field name: {stream_field_name} sensor type {sensor_type}')
                if stream_field_name in sensor_field_names:
                    measurement_stream_id = stream.get('stream_id')
                    self.measurement_stream_assigner.reassign_stream(measurement_stream_id, cloned_location_id)
