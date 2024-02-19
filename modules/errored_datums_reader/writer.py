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
                cursor.execute(sql, params)
        db.connection.commit()
