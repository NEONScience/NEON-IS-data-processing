from contextlib import closing


def get_parent(connection, named_location_id):
    """
    Get the parent ID of a named location.
    :param connection:
    :param named_location_id:
    :return:
    """
    sql = 'select prnt_nam_locn_id from nam_locn_tree where chld_nam_locn_id = :id'
    with closing(connection.cursor()) as cursor:
        cursor.prepare(sql)
        cursor.execute(None, id=named_location_id)
        row = cursor.fetchone()
        parent_id = row[0]
    return parent_id


def assign_parent(connection, parent_id, child_id):
    """
    Assign a parent to a new named location ID.
    :param connection: 
    :param parent_id: 
    :param child_id: 
    :return: 
    """
    sql = 'insert into nam_locn_tree (prnt_nam_locn_id, chld_nam_locn_id) values (:parent_id, :child_id)'
    with closing(connection.cursor()) as cursor:
        cursor.prepare(sql)
        cursor.execute(None, parent_id=parent_id, child_id=child_id)
        connection.commit()


def assign_to_same_parent(connection, source_named_location_id, assignable_named_location_id):
    parent_id = get_parent(connection, source_named_location_id)
    assign_parent(connection, parent_id, assignable_named_location_id)


def get_location_tree(connection, parent_id, child_id):
    sql = '''select prnt_nam_locn_id, chld_nam_locn_id 
                from nam_locn_tree
                where chld_nam_locn_id = :child_id
                and prnt_nam_locn_id = :parent_id
            '''
    entry = {}
    with closing(connection.cursor()) as cursor:
        cursor.prepare(sql)
        rows = cursor.execute(None, parent_id=parent_id, child_id=child_id)
        for row in rows:
            parent_id = row[0]
            child_id = row[1]
            entry = {'parent_id': parent_id, 'child_id': child_id}
    return entry
