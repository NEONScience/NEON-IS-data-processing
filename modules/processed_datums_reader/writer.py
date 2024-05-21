from collections import defaultdict
from contextlib import closing
from datetime import datetime
from processed_datums_reader.db_connector import Db


def write_to_db(db: Db, files_by_pipeline: defaultdict(lambda: defaultdict(int))) -> None:
    
    sql = f'''
        insert into {db.schema}.processed_datums 
            (dag_name, pipeline_name, processed_date, processed_group_count) 
        values 
            (%(dag_name)s, %(pipeline_name)s, %(processed_date)s, %(processed_group_count)s) 
    '''
    with closing(db.connection.cursor()) as cursor:
        for pipeline_name, processed_date in files_by_pipeline.items():
            dag_name = pipeline_name.split('_')[0]
            for processed_date, file_count in processed_date.items():
                date_object = datetime.strptime(processed_date, '%Y-%m-%d').date()
                params = dict(dag_name=dag_name, pipeline_name=pipeline_name, processedd_date=date_object,
                              processed_group_count=file_count)
                #print(sql.as_string(db.connection))
                clear_existing_records(db=db, dag_name=dag_name, pipeline_name=pipeline_name, processed_date=processed_date)
                cursor.execute(sql, params)
        db.connection.commit()
        
def clear_existing_records(db: Db, dag_name: str, pipeline_name: str, processed_date: str) -> None:
    
    date_object = datetime.strptime(processed_date, '%Y-%m-%d').date()
    sql = f'''
        delete from {db.schema}.processed_datums 
        where 
           dag_name = %(dag_name)s  
        and 
           pipeline_name = %(pipeline_name)s
        and 
            processed_date = %(date_object)s
    '''
    with closing(db.connection.cursor()) as cursor:
       # print(sql.as_string(db.connection))

        cursor.execute(sql, dict(dag_name=dag_name, pipeline_name=pipeline_name, processed_date=date_object))
        db.connection.commit()
