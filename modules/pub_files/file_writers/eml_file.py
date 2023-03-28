from datetime import datetime
from pathlib import Path
from typing import NamedTuple, Callable

import structlog
from xsdata.formats.dataclass.parsers import XmlParser
from xsdata.formats.dataclass.serializers import XmlSerializer
from xsdata.formats.dataclass.serializers.config import SerializerConfig

from eml.eml_2_2_0 import Eml, I18NNonEmptyStringType

from pub_files.database_queries.named_locations import NamedLocation
from pub_files.geometry import Geometry
from pub_files.input_files.file_metadata import FileMetadata


log = structlog.get_logger()


def format_date(date: datetime) -> str:
    return date.strftime('%Y%m%d')


class EmlDatabase(NamedTuple):
    get_named_location: Callable[[str], NamedLocation]
    get_geometry: Callable[[str], Geometry]


class EmlFile:

    # TODO: domain namedlocation, site namedlocation, site geometry
    def __init__(self, out_path: Path, metadata: FileMetadata, boilerplate: str, timestamp: datetime,
                 database: EmlDatabase):
        self.out_path = out_path
        self.metadata = metadata
        self.timestamp = timestamp
        self.database = database
        parser = XmlParser()
        self.eml = parser.from_string(boilerplate, Eml)

    def write(self) -> str:
        filename = self.get_filename()
        elements = self.metadata.path_elements
        path = Path(self.out_path, elements.site, elements.year, elements.month, filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        content = self.get_content()
        path.write_text(content)
        log.debug(f'\n\ncontent: \n\n{content}\n')
        return filename

    def add_dataset_id(self, product_id: str) -> None:
        dataset = self.eml.dataset
        dataset.id = product_id
        dataset.short_name = f'NEON{self.metadata.path_elements.site} {self.metadata.data_product.name} '

    def add_dataset_title(self) -> None:
        site = self.metadata.path_elements.site
        domain = self.metadata.path_elements.domain
        data_product_name = self.metadata.data_product.name
        domain_location = self.database.get_named_location(domain)
        title = f'NEON {data_product_name} at {site}, {domain_location.description}, '
        self.eml.dataset.title = I18NNonEmptyStringType(title)

    def get_content(self) -> str:
        self.add_dataset_id(self.metadata.path_elements.data_product_id)
        self.add_dataset_title()
        log.debug(f'\n\neml:\n\n{self.eml}\n')
        config = SerializerConfig(pretty_print=True)
        serializer = XmlSerializer(config=config)
        return serializer.render(self.eml)

    def get_filename(self) -> str:
        elements = self.metadata.path_elements
        product_id = self.metadata.data_product.short_data_product_id
        domain = self.metadata.path_elements.domain
        site = elements.site
        start = format_date(self.metadata.data_files.min_time)
        end = format_date(self.metadata.data_files.max_time)
        return f'NEON.{domain}.{site}.{product_id}.EML.{start}-{end}.{self.timestamp}.xml'
