import os

import unittest
import cx_Oracle
from contextlib import closing

import water_quality_cloning.named_location_creator as named_location_creator
import water_quality_cloning.asset_assigner as asset_assigner
import water_quality_cloning.location_tree_assigner as location_tree_assigner


class AppTest(unittest.TestCase):

    def setUp(self):
        #  Database URL in the form: [user]/[pass]@[url]:[port]/[sid]
        self.db_url = os.getenv('DATABASE_URL')

    def test_get_named_location(self):
        with closing(cx_Oracle.connect(self.db_url)) as db_connection:
            named_location = named_location_creator.get_named_location(db_connection, 156303)
        key = named_location.get('key')
        name = named_location.get('name')
        description = named_location.get('description')
        type_id = named_location.get('type_id')
        self.assertTrue(key == 156303)
        self.assertTrue(name == 'CFGLOC103495')
        self.assertTrue(description == 'Oksrukuyik Creek Water Chemistry and Temperature S2')
        self.assertTrue(type_id == 32)

    def test_get_new_name(self):
        new_name = named_location_creator.get_clone_name(1000)
        self.assertTrue(new_name == 'SENSOR001000')
        new_name = named_location_creator.get_clone_name(999)
        self.assertTrue(new_name == 'SENSOR000999')
        new_name = named_location_creator.get_clone_name(100)
        self.assertTrue(new_name == 'SENSOR000100')
        new_name = named_location_creator.get_clone_name(99)
        self.assertTrue(new_name == 'SENSOR000099')
        new_name = named_location_creator.get_clone_name(10)
        self.assertTrue(new_name == 'SENSOR000010')
        new_name = named_location_creator.get_clone_name(9)
        self.assertTrue(new_name == 'SENSOR000009')

    def test_clone(self):
        with closing(cx_Oracle.connect(self.db_url)) as db_connection:
            source_named_location = named_location_creator.get_named_location(db_connection, 156303)
            cloned_named_location = named_location_creator.create_clone(source_named_location, 10)
            key = cloned_named_location.get('key')
            name = cloned_named_location.get('name')
            description = cloned_named_location.get('description')
            type_id = cloned_named_location.get('type_id')
        self.assertTrue(key == 156303)
        self.assertTrue(name == 'SENSOR000010')
        self.assertTrue(description == 'Oksrukuyik Creek Water Chemistry and Temperature S2')
        self.assertTrue(type_id == 32)

    @unittest.skip('Fails if already loaded in the database.')
    def test_save_clone(self):

        source_key = 156303
        source_description = 'Oksrukuyik Creek Water Chemistry and Temperature S2'
        source_type_id = 32

        clone_index = 10
        expected_clone_name = 'SENSOR000010'

        with closing(cx_Oracle.connect(self.db_url)) as connection:
            # create the clone
            source_named_location = named_location_creator.get_named_location(connection, source_key)
            cloned_named_location = named_location_creator.create_clone(source_named_location, clone_index)
            clone_key = cloned_named_location.get('key')
            clone_name = cloned_named_location.get('name')
            clone_description = cloned_named_location.get('description')
            clone_type_id = cloned_named_location.get('type_id')

            self.assertTrue(clone_key == source_key)
            self.assertTrue(clone_name == expected_clone_name)
            self.assertTrue(clone_description == source_description)
            self.assertTrue(clone_type_id == source_type_id)

            # save the clone
            clone_key = named_location_creator.save_clone(connection, cloned_named_location)

            # check the clone
            clone_from_db = named_location_creator.get_named_location(connection, clone_key)
            clone_db_name = clone_from_db.get('name')
            clone_db_description = clone_from_db.get('description')
            clone_db_type_id = clone_from_db.get('type_id')
            self.assertTrue(clone_db_name == expected_clone_name)
            self.assertTrue(clone_db_description == source_description)
            self.assertTrue(clone_db_type_id == source_type_id)

            # delete the clone
            with closing(connection.cursor()) as delete_cursor:
                sql = 'delete from nam_locn where nam_locn_id = :key'
                delete_cursor.prepare(sql)
                delete_cursor.execute(None, key=clone_key)
                connection.commit()

            # check clone deleted
            deleted_clone = named_location_creator.get_named_location(connection, clone_key)
            self.assertTrue(deleted_clone is None)

    def test_asset_assigner_sensor_type_index(self):
        index = asset_assigner.get_clone_location_index('exo2')
        self.assertTrue(index == 0)

    def test_get_field_names(self):
        field_names = asset_assigner.get_field_names('exofdom')
        expected = ['fDOMRaw', 'fDOM']
        self.assertTrue(field_names == expected)

    def test_location_tree_assign_parent(self):
        named_location_id = 156303
        child_id = 100

        with closing(cx_Oracle.connect(self.db_url)) as connection:
            parent_id = location_tree_assigner.get_parent(connection, named_location_id)

            # create entry
            location_tree_assigner.assign_to_same_parent(connection, named_location_id, child_id)

            # get entry
            entry = location_tree_assigner.get_location_tree(connection, parent_id, child_id)
            self.assertTrue(len(entry) == 2)  # should contain the parent and child IDs.

            # clean up
            with closing(connection.cursor()) as delete_cursor:
                delete_sql = \
                    'delete from nam_locn_tree where prnt_nam_locn_id = :parent_id and chld_nam_locn_id = :child_id'
                delete_cursor.prepare(delete_sql)
                delete_cursor.execute(None, parent_id=parent_id, child_id=child_id)
                connection.commit()

            deleted_entry = location_tree_assigner.get_location_tree(connection, parent_id, child_id)
            self.assertTrue(len(deleted_entry) == 0)


if __name__ == '__main__':
    unittest.main()
