from collections import defaultdict
from contextlib import closing

from errored_datums_reader.db_connector import Db


def write_to_db(db: Db, files_by_pipeline: defaultdict[str, list[str]]) -> None:
    sql = f'''
        insert into {db.schema}.errored_datums 
            (dag_name, pipeline_name, file_path) 
        values 
            (%(dag_name)s, %(pipeline_name)s, %(file_path)s) 
    '''
    with closing(db.connection.cursor()) as cursor:
        for pipeline_name in files_by_pipeline.keys():
            dag_name = pipeline_name.split('_')[0]
            for file_path in files_by_pipeline[pipeline_name]:
                params = dict(dag_name=dag_name, pipeline_name=pipeline_name, file_path=file_path)
                clear_existing_records(db=db, dag_name=dag_name, pipeline_name=pipeline_name)
                cursor.execute(sql, params)
        db.connection.commit()
        
def clear_existing_records(db: Db, dag_name: str, pipeline_name: str) -> None:
    sql = f'''
        delete from {db.schema}.errored_datums 
        where 
           dag_name = %(dag_name)s 
        and 
           pipeline_name = %(pipeline_name)s
    '''
    with closing(db.connection.cursor()) as cursor:
        cursor.execute(sql, dict(dag_name=dag_name, pipeline_name=pipeline_name))
        db.connection.commit()
