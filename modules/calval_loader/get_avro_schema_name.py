#!/usr/bin/env python3
from contextlib import closing
from typing import Optional
import logging


def get_avro_schema_name(connection, asset_uid : int) -> Optional[list]:

    sql = '''
         select 
            distinct(ist.avro_schema_name )
         from 
            is_sensor_type ist, is_asset_definition iad2, is_asset_assignment iaa2
        where 
            iad2.asset_definition_uuid = iaa2.asset_definition_uuid 
        and 
            ist.sensor_type_name = iad2.sensor_type_name 
        and 
            iaa2.asset_uid = %(asset_uid)s
    '''
    with closing(connection.cursor()) as cursor:
        #print('avro schema sql is', sql)
        cursor.execute(sql, dict(asset_uid=asset_uid))
        row = cursor.fetchone()
        if row is None:
            logging.error(f'Avro schema name not found for asset id ID {asset_uid} .')
            return None
        avro_schema_name = row
        #avro_schema_name = row[0]        
        #if avro_schema name is exoconductivity, replace with exo2conductivity
        if avro_schema_name == 'exo':
            avro_schema_name = 'exo2'
        if avro_schema_name == 'exoconductivity':
            avro_schema_name = 'exo2conductivity'
        if avro_schema_name == 'exoturbidity':
            avro_schema_name = 'exo2turbidity'
        if avro_schema_name == 'exofdom':
            avro_schema_name = 'exo2fdom'
        if avro_schema_name == 'exophorp':
            avro_schema_name = 'exo2phorp'
        if avro_schema_name == 'exodissolvedoxygen':
            avro_schema_name = 'exo2dissolvedoxygen'
        if avro_schema_name == 'exoalgae':
            avro_schema_name = 'exo2algae'
        print(f'asset_uid: {asset_uid}, avro_schema_name: {avro_schema_name}')
    return avro_schema_name
