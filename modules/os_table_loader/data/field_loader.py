from contextlib import closing
from typing import NamedTuple, Optional

import psycopg2.extras

from data_access.db_connector import DbConnector
from os_table_loader.data.table_loader import Table


class Field(NamedTuple):
    field_id: int
    table_id: int
    rank: int
    data_product_number: Optional[str]
    field_name: str
    description: str
    data_type_code: str
    meas_scale: str
    ontology_mapping: Optional[str]
    pub_format: str
    example_entry: Optional[str]
    field_type: str
    inputs: Optional[str]
    ingest_source_type: str
    ingest_source_type_param: Optional[str]
    time_index: Optional[int]
    time_description: Optional[str]
    download_package: str
    data_category: Optional[str]
    sample_info: Optional[str]
    lov_code: Optional[str]
    primary_key: str
    unit_name: str
    redaction_flag: Optional[str]


def get_fields(connector: DbConnector, table: Table) -> list[Field]:
    fields: list[Field] = []
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'''
        select
            pub_field_def_id,
            pub_table_def_id,
            rank,
            dp_number, 
            field_name,
            description,
            data_type_code,
            meas_scale,
            ontology_mapping,
            pub_format,
            example_entry,
            field_type,
            inputs,
            ingest_source_type,
            ingest_source_type_param,
            time_index,
            time_description,
            download_package,
            data_category,
            sample_info,
            lov_code,
            primary_key,
            unit_name,
            redaction_flag
        from 
            {schema}.pub_field_def 
        where
            pub_table_def_id = %s
        order by rank
    '''
    with closing(connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)) as cursor:
        cursor.execute(sql, [table.id])
        rows = cursor.fetchall()
        for row in rows:
            field_id = row['pub_field_def_id']
            table_id = row['pub_table_def_id']
            rank = row['rank']
            data_product_number = row['dp_number']
            field_name = row['field_name']
            description = row['description']
            data_type_code = row['data_type_code']
            meas_scale = row['meas_scale']
            ontology_mapping = row['ontology_mapping']
            pub_format = row['pub_format']
            example_entry = row['example_entry']
            field_type = row['field_type']
            inputs = row['inputs']
            ingest_source_type = row['ingest_source_type']
            ingest_source_type_param = row['ingest_source_type_param']
            time_index = row['time_index']
            time_description = row['time_description']
            download_package = row['download_package']
            data_category = row['data_category']
            sample_info = row['sample_info']
            lov_code = row['lov_code']
            primary_key = row['primary_key']
            unit_name = row['unit_name']
            redaction_flag = row['redaction_flag']
            field = Field(field_id=field_id,
                          table_id=table_id,
                          rank=rank,
                          data_product_number=data_product_number,
                          field_name=field_name,
                          description=description,
                          data_type_code=data_type_code,
                          meas_scale=meas_scale,
                          ontology_mapping=ontology_mapping,
                          pub_format=pub_format,
                          example_entry=example_entry,
                          field_type=field_type,
                          inputs=inputs,
                          ingest_source_type=ingest_source_type,
                          ingest_source_type_param=ingest_source_type_param,
                          time_index=time_index,
                          time_description=time_description,
                          download_package=download_package,
                          data_category=data_category,
                          sample_info=sample_info,
                          lov_code=lov_code,
                          primary_key=primary_key,
                          unit_name=unit_name,
                          redaction_flag=redaction_flag)
            fields.append(field)
    return fields
