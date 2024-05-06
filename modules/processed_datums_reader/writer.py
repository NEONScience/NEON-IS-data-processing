from collections import defaultdict
from contextlib import closing

from processed_datums_reader.db_connector import Db


def write_to_db(db: Db, files_by_pipeline: defaultdict(lambda: defaultdict(int))) -> None:
    sql = f'''
        insert into {db.schema}.processed_datums 
            (dag_name, pipeline_name, group_name, processed_file_count) 
        values 
            (%(dag_name)s, %(pipeline_name)s, %(group_name)s, %(processed_file_count)s) 
    '''
    with closing(db.connection.cursor()) as cursor:
        for pipeline_name, group_name_dict in files_by_pipeline.items():
            dag_name = pipeline_name.split('_')[0]
            for group_key, file_count in group_name_dict.items():
                params = dict(dag_name=dag_name, pipeline_name=pipeline_name, group_name=group_key,
                              processed_file_count=file_count)
                clear_existing_records(db=db, dag_name=dag_name, pipeline_name=pipeline_name, group_name=group_key)
                cursor.execute(sql, params)
        db.connection.commit()
        
def clear_existing_records(db: Db, dag_name: str, pipeline_name: str, group_name: str) -> None:
    sql = f'''
        delete from {db.schema}.errored_datums 
        where 
           dag_name = %(dag_name)s  
        and 
           pipeline_name = %(pipeline_name)s
        and 
            group_name = %(group_name)s
    '''
    with closing(db.connection.cursor()) as cursor:
        cursor.execute(sql, dict(dag_name=dag_name, pipeline_name=pipeline_name, group_name=group_name))
        db.connection.commit()
