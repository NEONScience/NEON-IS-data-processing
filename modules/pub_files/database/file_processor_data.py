from functools import partial

from data_access.db_connector import DbConnector
from pub_files.database.queries.data_products import get_data_product
from pub_files.input_files.file_processor import FileProcessorDatabase


class FileProcessorData:

    def __init__(self, connector: DbConnector) -> None:
        self.get_data_product = partial(get_data_product, connector)
        self.file_processor_database = FileProcessorDatabase(get_data_product=self.get_data_product)

    def get_database(self) -> FileProcessorDatabase:
        return self.file_processor_database
