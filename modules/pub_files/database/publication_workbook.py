from contextlib import closing
from typing import NamedTuple, List, Dict

from psycopg2.extras import DictCursor

from data_access.db_connector import DbConnector


def get_file_key(table_name: str, download_package: str) -> str:
    """Create a key to associate a particular data file with its workbook description."""
    return f'{table_name}.{download_package}'


class WorkbookRow(NamedTuple):
    """Class to represent a single row in a workbook."""
    table_name: str
    field_name: str
    description: str
    data_type_code: str
    measurement_scale: str
    publication_format: str
    download_package: str
    unit_name: str
    lov_code: str
    table_description: str


class PublicationWorkbook:

    def __init__(self, workbook_rows: List[WorkbookRow], file_descriptions: Dict[str, str]):
        self.workbook_rows = workbook_rows
        self.file_descriptions = file_descriptions

    def get_file_description(self, table_name: str, download_package: str) -> str:
        """
        Returns a description for a file containing data for a particular workbook table name
        and download package type.
        """
        return self.file_descriptions[get_file_key(table_name, download_package)]


def get_workbook(connector: DbConnector, data_product_id: str) -> PublicationWorkbook:
    """Read a publication workbook from the database for the given data product identifier."""
    workbook_rows: List[WorkbookRow] = []
    file_descriptions: Dict[str, str] = {}
    schema = connector.get_schema()
    connection = connector.get_connection()
    sql = f'''
        select
            pfd.field_name,
            pfd.description,
            pfd.data_type_code,
            pfd.meas_scale,
            pfd.pub_format,
            pfd.download_package,
            pfd.unit_name,
            pfd.lov_code,
            ptd.name as table_name,
            ptd.description as table_description
        from 
            {schema}.pub_field_def pfd, {schema}.pub_table_def ptd
        where 
            pfd.pub_table_def_id = ptd.pub_table_def_id 
        and 
            ptd.dp_idq = %s
        order by
            table_name
    '''
    with closing(connection.cursor(cursor_factory=DictCursor)) as cursor:
        cursor.execute(sql, [data_product_id])
        rows = cursor.fetchall()
        for row in rows:
            field_name = row['field_name']
            description = row['description']
            data_type_code = row['data_type_code']
            measurement_scale = row['meas_scale']
            publication_format = row['pub_format']
            download_package = row['download_package']
            unit_name = row['unit_name']
            lov_code = row['lov_code']
            table_name = row['table_name']
            table_description = row['table_description']
            file_descriptions[get_file_key(table_name, download_package)] = table_description
            workbook_row = WorkbookRow(table_name=table_name,
                                       field_name=field_name,
                                       description=description,
                                       data_type_code=data_type_code,
                                       measurement_scale=measurement_scale,
                                       publication_format=publication_format,
                                       download_package=download_package,
                                       unit_name=unit_name,
                                       lov_code=lov_code,
                                       table_description=table_description)
            workbook_rows.append(workbook_row)
    return PublicationWorkbook(workbook_rows, file_descriptions)
