from datetime import datetime
from pathlib import Path
from typing import NamedTuple, Callable

import structlog
from xsdata.formats.dataclass.parsers import XmlParser
from xsdata.formats.dataclass.serializers import XmlSerializer
from xsdata.formats.dataclass.serializers.config import SerializerConfig

import eml.eml_2_2_0 as eml

from pub_files.output_files.eml.eml_coverage import EmlCoverage
from pub_files.output_files.eml.eml_database import EmlDatabase
from pub_files.output_files.eml.measurement_scale import MeasurementScale
from pub_files.geometry import Geometry
from pub_files.input_files.file_metadata import FileMetadata
from pub_files.publication_workbook import PublicationWorkbook

log = structlog.get_logger()


# TODO: Put template in Github.
def wrap(element: str) -> str:
    return f'''
        <eml:eml packageId=""
                 scope="system" 
                 system="https://www.neonscience.org" 
                 xmlns:eml="https://eml.ecoinformatics.org/eml-2.2.0" 
                 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
                 xmlns:stmml="http://www.xml-cml.org/schema/stmml-1.2" 
                 xsi:schemaLocation="https://eml.ecoinformatics.org/eml-2.2.0 eml.xsd">
            <dataset>
                {element}
            </dataset>
        </eml:eml>
    '''


def format_date(date: datetime) -> str:
    return date.strftime('%Y%m%d')


class EmlFiles(NamedTuple):
    get_boilerplate: Callable[[], str]
    get_contact: Callable[[], str]
    get_intellectual_rights: Callable[[], str]
    get_unit_types: Callable[[], str]


class EmlFile:

    def __init__(self,
                 out_path: Path,
                 metadata: FileMetadata,
                 eml_files: EmlFiles,
                 publication_workbook: PublicationWorkbook,
                 package_type: str,
                 timestamp: datetime,
                 database: EmlDatabase) -> None:
        self.out_path = out_path
        self.metadata: FileMetadata = metadata
        self.eml_files: EmlFiles = eml_files
        self.publication_workbook = publication_workbook
        self.package_type = package_type
        self.timestamp: datetime = timestamp
        self.database: EmlDatabase = database
        parser = XmlParser()
        self.eml = parser.from_string(eml_files.get_boilerplate(), eml.Eml)

    def write(self) -> str:
        filename = self.get_filename()
        elements = self.metadata.path_elements
        path = Path(self.out_path, elements.site, elements.year, elements.month, filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        content = self.create_file_content()
        path.write_text(content)
        log.debug(f'\n\ncontent: \n\n{content}\n')
        return filename

    def set_dataset_id(self, product_id: str) -> None:
        dataset = self.eml.dataset
        dataset.id.append(product_id)
        dataset.short_name = f'NEON {self.metadata.path_elements.site} {self.metadata.data_product.name} '

    def set_dataset_title(self) -> None:
        site = self.metadata.path_elements.site
        domain = self.metadata.path_elements.domain
        data_product_name = self.metadata.data_product.name
        domain_location = self.database.get_named_location(domain)
        title = f'NEON {data_product_name} at {site}, {domain_location.description}, '
        self.eml.dataset.title.append(eml.I18NNonEmptyStringType(title))

    def add_creator(self) -> eml.ResponsibleParty:
        contact_eml = self.eml_files.get_contact()
        string_type = eml.I18NNonEmptyStringType(wrap(contact_eml))
        person = eml.Person([string_type])
        creator = eml.ResponsibleParty([person])
        self.eml.dataset.creator.append(creator)
        # Used by other elements.
        creator_without_id = eml.ResponsibleParty()
        creator_without_id.individual_name.extend(creator.individual_name)
        creator_without_id.address.extend(creator.address)
        creator_without_id.phone.extend(creator.phone)
        creator_without_id.electronic_mail_address.extend(creator.electronic_mail_address)
        creator_without_id.online_url.extend(creator.online_url)
        return creator_without_id

    def set_coverage(self) -> None:
        coverage = eml.Coverage()
        site_name = self.metadata.path_elements.site
        site_geometry: Geometry = self.database.get_geometry(site_name)
        if site_geometry:
            geographic_coverage = EmlCoverage(site_geometry, self.metadata, self.database).get_coverage()
            coverage.geographic_coverage = geographic_coverage
        self.eml.dataset.coverage = coverage

    def set_dataset(self, creator_without_id) -> None:
        self.eml.dataset.purpose = self.metadata.data_product.description
        self.eml.dataset.contact = creator_without_id
        self.eml.dataset.publisher = creator_without_id
        citation = self.eml.dataset.project.study_area_description.citation[0]
        citation.creator = creator_without_id
        citation.report.publisher = creator_without_id

    def set_dataset_temporality(self) -> None:
        start_time = self.metadata.data_files.min_time
        end_time = self.metadata.data_files.max_time
        start_date = datetime.strftime(start_time, 'YYYYmmdd')
        end_date = datetime.strftime(end_time, 'YYYYmmdd')
        start_date_dashed = datetime.strftime(start_time, 'YYYY-mm-dd')
        end_date_dashed = datetime.strftime(end_time, 'YYYY-mm-dd')
        dataset_id = f'{self.eml.dataset.id[0]}{start_date}-{end_date}'
        self.eml.dataset.id.clear()
        self.eml.dataset.id.append(dataset_id)
        self.eml.dataset.short_name = f'{self.eml.dataset.short_name}{start_date_dashed} to {end_date_dashed}'

        title = f'{self.eml.dataset.title[0].content}{start_date_dashed} to {end_date_dashed}'
        self.eml.dataset.title[0].content = title
        self.set_temporal_coverage(start_date_dashed, end_date_dashed)

    def set_temporal_coverage(self, start_date_dashed, end_date_dashed) -> None:
        begin_date = eml.SingleDateTimeType()
        end_date = eml.SingleDateTimeType()
        begin_date.calendar_date = start_date_dashed
        end_date.calendar_date = end_date_dashed
        range_of_dates = eml.TemporalCoverageRangeOfDates()
        range_of_dates.begin_date = begin_date
        range_of_dates.end_date = end_date
        temporal_coverage = eml.TemporalCoverage()
        temporal_coverage.range_of_dates = range_of_dates
        self.eml.dataset.coverage.temporal_coverage = temporal_coverage

    def set_data_tables(self) -> None:
        for file in self.metadata.data_files.files:
            data_table = eml.DataTable()
            data_table.entity_name = Path(file.filename).stem
            data_table.case_sensitive = eml.DataTableTypeCaseSensitive.YES
            data_table.number_of_records = file.line_count
            attribute_list = eml.AttributeList()
            for row in self.publication_workbook.get_workbook():
                package_type = self.publication_workbook.get_download_package(row)
                if package_type != self.package_type:
                    continue
                field_name = self.publication_workbook.get_field_name(row)
                table_description = self.publication_workbook.get_table_description(row)
                attribute = eml.Attribute()
                attribute.attribute_name = field_name
                attribute.attribute_definition = table_description
                measurement_scale = MeasurementScale(self.publication_workbook, self.metadata, self.database)
                attribute.measurement_scale = measurement_scale.get_measurement_scale(row)
                attribute_list.attribute.append(attribute)
            data_table.attribute_list = attribute_list
            self.eml.dataset.data_table.append(data_table)

    def create_file_content(self) -> str:
        self.set_dataset_id(self.metadata.path_elements.data_product_id)
        self.set_dataset_title()
        creator_without_id = self.add_creator()
        self.eml.dataset.metadata_provider.append(creator_without_id)
        self.eml.dataset.pub_date = datetime.strftime(self.timestamp, 'YYYY-mm-dd')
        self.eml.dataset.language = eml.I18NNonEmptyStringType('English')
        self.eml.dataset.intellectual_rights = self.eml_files.get_intellectual_rights()
        self.set_coverage()
        self.set_dataset(creator_without_id)
        self.set_dataset_temporality()
        self.set_data_tables()
        self.add_additional_metadata()
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

    def add_additional_metadata(self):
        additional_metadata = eml.EmlAdditionalMetadata()
        additional_metadata_content: str = self.eml_files.get_unit_types()
        additional_metadata.metadata = additional_metadata_content
        self.eml.additional_metadata = [additional_metadata]
