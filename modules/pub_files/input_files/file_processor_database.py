from typing import NamedTuple, Callable

from pub_files.data_product import DataProduct


class FileProcessorDatabase(NamedTuple):
    get_data_product: Callable[[str], DataProduct]
