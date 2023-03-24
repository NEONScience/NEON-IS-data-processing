from functools import partial

from data_access.db_connector import DbConnector
from pub_files.database_queries.data_products import get_data_product
from pub_files.database_queries.data_product_keywords import get_keywords
from pub_files.database_queries.file_descriptions import get_descriptions
from pub_files.database_queries.geolocation_geometry import get_geometry
from pub_files.database_queries.log_entries import get_log_entries
from pub_files.database_queries.named_locations import get_named_location
from pub_files.database_queries.geolocations import get_geolocations
from pub_files.file_writers.eml_file import EmlDatabase
from pub_files.file_writers.readme.readme_file import ReadmeDatabase
from pub_files.file_writers.sensor_positions_file import SensorPositionsDatabase
from pub_files.input_files.file_processor import FileProcessorDatabase


class Database:

    def __init__(self, connector: DbConnector):
        self.get_descriptions = partial(get_descriptions, connector)
        self.get_data_product = partial(get_data_product, connector)
        self.get_geolocations = partial(get_geolocations, connector)
        self.get_geometry = partial(get_geometry, connector)
        self.get_named_location = partial(get_named_location, connector)
        self.get_keywords = partial(get_keywords, connector)
        self.get_log_entries = partial(get_log_entries, connector)

    def file_processor(self):
        return FileProcessorDatabase(get_descriptions=self.get_descriptions,
                                     get_data_product=self.get_data_product)

    def readme(self):
        return ReadmeDatabase(get_geometry=self.get_geometry,
                              get_keywords=self.get_keywords,
                              get_log_entries=self.get_log_entries)

    def sensor_positions(self):
        return SensorPositionsDatabase(get_geolocations=self.get_geolocations,
                                       get_geometry=self.get_geometry,
                                       get_named_location=self.get_named_location)

    def eml(self):
        return EmlDatabase(get_named_location=self.get_named_location, get_geometry=self.get_geometry)
