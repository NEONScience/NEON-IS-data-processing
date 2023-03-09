"""
This module reads data from a database by wrapping external
functions to hide the database connection from clients.
"""
from data_access.db_connector import DbConnector
from publication_files_generator.database_queries.data_product import get_data_product
from publication_files_generator.database_queries.data_product_keywords import get_keywords
from publication_files_generator.database_queries.file_descriptions import get_descriptions
from publication_files_generator.data_store import DataStore
from publication_files_generator.database_queries.location_geometry import get_geometry
from publication_files_generator.database_queries.log_entries import get_log_entries


def get_data_store(connector: DbConnector):
    return DataStore(
        get_data_product=_get_data_product(connector),
        get_keywords=_get_keywords(connector),
        get_log_entries=_get_log_entries(connector),
        get_geometry=_get_geometry(connector),
        get_descriptions=_get_descriptions(connector))


def _get_data_product(connector: DbConnector):
    def f(data_product_id: str):
        return get_data_product(connector, data_product_id)
    return f


def _get_keywords(connector: DbConnector):
    def f(data_product_id: str):
        return get_keywords(connector, data_product_id)
    return f


def _get_log_entries(connector: DbConnector):
    def f(data_product_id: str):
        return get_log_entries(connector, data_product_id)
    return f


def _get_geometry(connector: DbConnector):
    def f(data_product_id: str):
        return get_geometry(connector, data_product_id)
    return f


def _get_descriptions(connector: DbConnector):
    def f():
        return get_descriptions(connector)
    return f
