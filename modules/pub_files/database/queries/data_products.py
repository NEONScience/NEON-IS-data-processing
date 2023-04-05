from contextlib import closing

from data_access.db_connector import DbConnector
from pub_files.data_product import DataProduct
from pub_files.database.queries.data_product_type_names import get_type_name


def get_data_product(connector: DbConnector, data_product_id: str) -> DataProduct:
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'''
         select
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
        cursor.execute(sql, [data_product_id])
        row = cursor.fetchone()
        name = row[0]
        description = row[1]
        category = row[2]
        supplier = row[3]
        short_name = row[4]
        abstract = row[5]
        design_description = row[6]
        study_description = row[7]
        sensor = row[8]
        basic_description = row[9]
        expanded_description = row[10]
        remarks = row[11]
    type_name = get_type_name(connector, data_product_id)
    return DataProduct(data_product_id=data_product_id,
                       name=name,
                       type_name=type_name,
                       description=description,
                       category=category,
                       supplier=supplier,
                       short_name=short_name,
                       abstract=abstract,
                       design_description=design_description,
                       study_description=study_description,
                       sensor=sensor,
                       basic_description=basic_description,
                       expanded_description=expanded_description,
                       remarks=remarks)
