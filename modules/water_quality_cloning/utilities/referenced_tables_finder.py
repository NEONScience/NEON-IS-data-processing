from contextlib import closing


def show_referenced_tables(db_connection):
    """
    Get all the tables connected to 'nam_locn'
    :param db_connection:
    :return:
    """
    sql = '''
        select * from all_constraints where r_constraint_name in
                (select constraint_name from all_constraints where table_name='NAM_LOCN')
        '''
    with closing(db_connection.cursor()) as cursor:
        cursor.prepare(sql)
        rows = cursor.execute(None)
        for row in rows:
            table_name = row[3]
            if 'OS' not in table_name and 'DP' not in table_name:
                print(table_name)
