"""
Module containing functions to read data product data from the database.
"""
from contextlib import closing
from typing import NamedTuple

from data_access.db_connector import DbConnector


class DataProduct(NamedTuple):
    idq: str
    short_idq: str
    name: str
    type_name: str
    description: str
    category: str
    supplier: str
    supplier_full_name: str
    short_name: str
    abstract: str
    design_description: str
    study_description: str
    sensor: str
    basic_description: str
    expanded_description: str
    remarks: str


def get_supplier_full_name(supplier: str) -> str:
    if supplier == 'TIS':
        return 'Terrestrial Instrument System'
    if supplier == 'TOS':
        return 'Terrestrial Observation System'
    if supplier == 'AOP':
        return 'Airborne Observation Platform'
    if supplier == 'AOS':
        return 'Aquatic Observation System'
    if supplier == 'AIS':
        return 'Aquatic Instrument System'
    return supplier


def get_type_name(connector: DbConnector, dp_idq: str) -> str:
    """Get the data product type."""
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'''
    select 
        type_name 
    from 
        {schema}."type" t, {schema}.dp_catalog 
    where 
        t.type_id = dp_catalog.type_id 
    and 
        dp_catalog.dp_idq = %s
    '''
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [dp_idq])
        row = cursor.fetchone()
        type_name = row[0]
    return type_name


def get_data_product(connector: DbConnector, dp_idq: str) -> DataProduct:
    """
    Get the data product metadata for the given IDQ.

    :param connector: A database connection.
    :param dp_idq: The data product idq.
    :return: The data product metadata.
    """
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'''
         select
             dp_idq,
             dp_name,
             dp_desc,
             category,
             supplier,
             dp_shortname,
             dp_abstract,
             design_desc,
             study_desc,
             sensor,
             basic_desc,
             expanded_desc,
             remarks    
         from
             {schema}.dp_catalog 
         where
             dp_idq = %s
    '''
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [dp_idq])
        row = cursor.fetchone()
        idq = row[0]
        name = row[1]
        description = row[2]
        category = row[3]
        supplier = row[4]
        short_name = row[5]
        abstract = row[6]
        design_description = row[7]
        study_description = row[8]
        sensor = row[9]
        basic_description = row[10]
        expanded_description = row[11]
        remarks = row[12]
    type_name = get_type_name(connector, idq)
    data_product = DataProduct(
        idq=idq,
        short_idq=idq.replace('NEON.DOM.SITE.', ''),
        name=name,
        type_name=type_name,
        description=description,
        category=category,
        supplier=supplier,
        supplier_full_name=get_supplier_full_name(supplier),
        short_name=short_name,
        abstract=abstract,
        design_description=design_description,
        study_description=study_description,
        sensor=sensor,
        basic_description=basic_description,
        expanded_description=expanded_description,
        remarks=remarks
    )
    return data_product
