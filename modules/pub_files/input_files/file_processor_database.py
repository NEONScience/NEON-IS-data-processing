from functools import partial
from typing import NamedTuple, Callable

from data_access.db_connector import DbConnector
from pub_files.data_product import DataProduct
from pub_files.database.data_products import get_data_product
from pub_files.database.publication_workbook import PublicationWorkbook, get_workbook


class FileProcessorDatabase(NamedTuple):
    get_data_product: Callable[[str], DataProduct]
    get_workbook: Callable[[str], PublicationWorkbook]


def get_file_processor_database(connector: DbConnector) -> FileProcessorDatabase:
    return FileProcessorDatabase(get_data_product=partial(get_data_product, connector),
                                 get_workbook=partial(get_workbook, connector))
