from functools import partial
from typing import NamedTuple

from data_access.db_connector import DbConnector
from publication_files.database_queries.data_product import get_data_product
from publication_files.database_queries.data_product_keywords import get_keywords
from publication_files.database_queries.file_descriptions import get_descriptions
from publication_files.database_queries.geolocation_geometry import get_geometry
from publication_files.database_queries.log_entries import get_log_entries
from publication_files.database_queries.named_location import get_named_location
from publication_files.database_queries.geolocations import get_geolocations
from publication_files.file_generators.readme_file import ReadmeDatabase
from publication_files.file_generators.sensor_positions_file import SensorPositionsDatabase
from publication_files.file_processor import FileProcessorDatabase


class Database(NamedTuple):
    file_processor_database: FileProcessorDatabase
    readme_database: ReadmeDatabase
    sensor_positions_database: SensorPositionsDatabase


def get_database(connector: DbConnector) -> Database:
    get_descriptions_partial = partial(get_descriptions, connector)
    get_data_product_partial = partial(get_data_product, connector)
    get_geolocations_partial = partial(get_geolocations, connector)
    get_geometry_partial = partial(get_geometry, connector)
    get_named_location_partial = partial(get_named_location, connector)
    get_keywords_partial = partial(get_keywords, connector)
    get_log_entries_partial = partial(get_log_entries, connector)
    file_processor_database = FileProcessorDatabase(get_descriptions=get_descriptions_partial)
    readme_database = ReadmeDatabase(get_data_product=get_data_product_partial,
                                     get_geometry=get_geometry_partial,
                                     get_keywords=get_keywords_partial,
                                     get_log_entries=get_log_entries_partial)
    sensor_positions_database = SensorPositionsDatabase(get_geolocations=get_geolocations_partial,
                                                        get_geometry=get_geometry_partial,
                                                        get_named_location=get_named_location_partial)
    return Database(file_processor_database=file_processor_database,
                    readme_database=readme_database,
                    sensor_positions_database=sensor_positions_database)
