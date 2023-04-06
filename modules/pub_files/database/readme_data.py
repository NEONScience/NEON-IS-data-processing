from functools import partial

from data_access.db_connector import DbConnector
from pub_files.database.queries.geolocation_geometry import get_geometry
from pub_files.database.queries.data_product_keywords import get_keywords
from pub_files.database.queries.log_entries import get_log_entries
from pub_files.output_files.readme.readme_database import ReadmeDatabase


class ReadmeData:

    def __init__(self, connector: DbConnector) -> None:
        self.get_geometry = partial(get_geometry, connector)
        self.get_keywords = partial(get_keywords, connector)
        self.get_log_entries = partial(get_log_entries, connector)

    def get_database(self) -> ReadmeDatabase:
        return ReadmeDatabase(get_geometry=self.get_geometry,
                              get_keywords=self.get_keywords,
                              get_log_entries=self.get_log_entries)
