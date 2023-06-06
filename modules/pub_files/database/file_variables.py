from contextlib import closing
from typing import List, NamedTuple

from psycopg2.extras import DictCursor

from data_access.db_connector import DbConnector


class FileVariables(NamedTuple):
    table_name: str
    description: str
    term_name: str
    rank: int
    download_package: str
    publication_format: str
    data_type: str
    units: str

# TODO: create separate modules for SAE and IS and configure what files are output in the Pachyderm pipeline spec.
def get_sensor_positions(connector: DbConnector) -> List[FileVariables]:
    return get_variables(connector, 'Sensor Positions File')


def get_is_science_review(connector: DbConnector) -> List[FileVariables]:
    return get_variables(connector, 'IS Science Review Flag File')


def get_sae_science_review(connector: DbConnector) -> List[FileVariables]:
    return get_variables(connector, 'SAE Science Review Flag File')


def get_variables(connector: DbConnector, file_name: str) -> List[FileVariables]:
    """Returns the variables for a publication file."""
    file_variables: List[FileVariables] = []
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'''
        select
            pf.table_name,
            pf.description,
            pff.rank,
            pff.term_name,
            pff.download_package,
            pff.pub_format,
            t.unit_name,
            t.data_type_code,
            t.term_description
        from
            {schema}.pub_file_field pff
        inner join
            {schema}.pub_file pf 
        on
            pf.id = pff.file_id
        and
            pf."name" = %s
        inner join 
	        {schema}.term t
        on
	        pff.term_name = t.term_name
	    order by
	        pf.table_name
    '''
    with closing(connection.cursor(cursor_factory=DictCursor)) as cursor:
        cursor.execute(sql, [file_name])
        rows = cursor.fetchall()
        for row in rows:
            table_name: str = row['table_name']
            description: str = row['term_description']
            term_name: str = row['term_name']
            rank: int = row['rank']
            download_package: str = row['download_package']
            publication_format: str = row['pub_format']
            data_type: str = row['data_type_code']
            units: str = row['unit_name']
            file_variables.append(FileVariables(table_name=table_name,
                                                description=description,
                                                term_name=term_name,
                                                rank=rank,
                                                download_package=download_package,
                                                publication_format=publication_format,
                                                data_type=data_type,
                                                units=units))
    return file_variables





