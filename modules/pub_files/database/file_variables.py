from contextlib import closing
from typing import List, NamedTuple

from data_access.db_connector import DbConnector


class FileVariables(NamedTuple):
    table_name: str
    description: str
    term_name: str
    rank: int
    download_package: str
    publication_format: str


def get_sensor_positions(connector: DbConnector) -> List[FileVariables]:
    return _get_variables(connector, 'Sensor Positions File')


def get_is_science_review(connector: DbConnector) -> List[FileVariables]:
    return _get_variables(connector, 'IS Science Review Flag File')


def get_sae_science_review(connector: DbConnector) -> List[FileVariables]:
    return _get_variables(connector, 'SAE Science Review Flag File')


def _get_variables(connector: DbConnector, file_name: str) -> List[FileVariables]:
    file_variables: List[FileVariables] = []
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'''
        select 
            table_name,
            description,
            term_name,
            rank,
            download_package,
            pub_format
        from 
            {schema}.pub_file_field pff
        inner join 
            {schema}.pub_file 
        on 
            {schema}.pub_file.id = pff.file_id 
        and 
            {schema}.pub_file."name" = %s
    '''
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [file_name])
        rows = cursor.fetchall()
        for row in rows:
            table_name: str = row[0]
            description: str = row[1]
            term_name: str = row[2]
            rank: int = row[3]
            download_package: str = row[4]
            publication_format: str = row[5]
            file_variables.append(FileVariables(table_name=table_name,
                                                description=description,
                                                term_name=term_name,
                                                rank=rank,
                                                download_package=download_package,
                                                publication_format=publication_format))
    return file_variables
