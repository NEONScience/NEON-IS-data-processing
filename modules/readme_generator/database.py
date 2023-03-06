"""
This module reads data from a database by wrapping external
functions in a class hiding the database connection from
clients.
"""
from data_access.db_connector import DbConnector
from readme_generator.data_product import get_data_product
from readme_generator.data_product_keyword import get_keywords
from readme_generator.file_descriptions import get_descriptions
from readme_generator.generator import DataFunctions
from readme_generator.location_geometry import get_geometry
from readme_generator.log_entry import get_log_entries


class Database:
    def __init__(self, connector: DbConnector):
        self.conn = connector

    def get_data_product(self, data_product_id: str):
        return get_data_product(self.conn, data_product_id)

    def get_keywords(self, data_product_id: str):
        return get_keywords(self.conn, data_product_id)

    def get_log_entries(self, data_product_id: str):
        return get_log_entries(self.conn, data_product_id)

    def get_geometry(self, data_product_id: str):
        return get_geometry(self.conn, data_product_id)

    def get_descriptions(self):
        return get_descriptions(self.conn)

    def get_data_functions(self):
        return DataFunctions(
            get_log_entries=self.get_log_entries,
            get_data_product=self.get_data_product,
            get_geometry=self.get_geometry,
            get_descriptions=self.get_descriptions,
            get_keywords=self.get_keywords)
