from contextlib import closing


def find_properties_for_cloning(db_connection):
    """
    NOTE: Properties are all PRT related and do not need to be cloned.
    Find rows that may need cloning (if they apply to WQ) in a table referencing nam_locn
    (this example looks at the property table)
    :param db_connection:
    :return:
    """
    sql = '''
        select * from property, attr
            where property.nam_locn_id in
                (select distinct nam_locn_id
                    from pdr.meas_strm
                    where (meas_strm_name like '%DP0%'
                        and meas_strm_name not like '%DP0.20033%'
                        and meas_strm_name not like '%DP0.20005%'
                        and nam_locn_id in
                            (select distinct nam_locn_id
                                from pdr.meas_strm
                                where (meas_strm_name like '%DP0.20033%'
                                    or meas_strm_name like '%DP0.20005%'))))
            join attr on property.attr_id = attr.attr_id
        '''
    with closing(db_connection.cursor()) as cursor:
        cursor.prepare(sql)
        rows = cursor.execute(None)
        for row in rows:
            # property_id = row[0]
            # attribute_id = row[1]
            print(row)
