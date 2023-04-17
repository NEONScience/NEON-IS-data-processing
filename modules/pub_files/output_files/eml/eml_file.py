from datetime import datetime
from pathlib import Path
from typing import Optional

import eml.eml_2_2_0 as eml
import structlog
from xsdata.formats.dataclass.parsers import XmlParser
from xsdata.formats.dataclass.serializers import XmlSerializer
from xsdata.formats.dataclass.serializers.config import SerializerConfig

import pub_files.output_files.eml.stmml.stmml_1_2 as stmml
from pub_files.geometry import Geometry
from pub_files.input_files.file_metadata import FileMetadata
from pub_files.output_files.eml.date_formats import DateFormats
from pub_files.output_files.eml.eml_coverage import EmlCoverage
from pub_files.output_files.eml.eml_database import EmlDatabase
from pub_files.output_files.eml.eml_measurement_scale import MeasurementScale
from pub_files.output_files.eml.external_eml_files import ExternalEmlFiles
from pub_files.output_files.eml.neon_units import NeonUnits
from pub_files.output_files.filename_format import format_timestamp
from pub_files.publication_workbook import PublicationWorkbook

log = structlog.get_logger()


class EmlFile:

    def __init__(self, out_path: Path, metadata: FileMetadata, eml_files: ExternalEmlFiles,
                 publication_workbook: PublicationWorkbook, package_type: str, timestamp: datetime,
                 database: EmlDatabase) -> None:
        self.out_path = out_path
        self.metadata: FileMetadata = metadata
        self.eml_files: ExternalEmlFiles = eml_files
        self.publication_workbook = publication_workbook
        self.package_type = package_type
        self.timestamp: datetime = timestamp
        self.database: EmlDatabase = database
        self.xml_parser = XmlParser()
        self.eml = self.xml_parser.from_string(eml_files.get_boilerplate(), eml.Eml)

    def write(self) -> str:
        self._add_content()
        content = self._render_content()
        log.debug(f'\n\nEML content:\n{content}\n')
        filename = self._get_filename()
        Path(self.out_path, filename).write_text(content)
        return filename

    def _render_content(self) -> str:
        config = SerializerConfig(pretty_print=True)
        serializer = XmlSerializer(config=config)
        return serializer.render(self.eml)

    def _add_content(self) -> None:
        self._set_dataset_id(self.metadata.path_elements.data_product_id)
        self._set_dataset_title()
        creator_without_id = self._set_creator()
        self.eml.dataset.metadata_provider.clear()
        self.eml.dataset.metadata_provider.append(creator_without_id)
        self.eml.dataset.pub_date = DateFormats.format_dashed_date(self.timestamp)
        self.eml.dataset.language = eml.I18NNonEmptyStringType().content = 'English'
        self.eml.dataset.purpose = self.metadata.data_product.description
        self.eml.dataset.contact = creator_without_id
        self.eml.dataset.publisher = creator_without_id
        self._set_intellectual_rights()
        self._set_coverage()
        self._set_citation(creator_without_id)
        self._set_dataset_id_title_dates()
        self._set_data_tables()
        self._set_additional_metadata()

    def _set_dataset_id(self, product_id: str) -> None:
        dataset = self.eml.dataset
        dataset.id.append(product_id)
        dataset.short_name = f'NEON {self.metadata.path_elements.site} {self.metadata.data_product.name}'

    def _set_dataset_title(self) -> None:
        site = self.metadata.path_elements.site
        domain = self.metadata.path_elements.domain
        data_product_name = self.metadata.data_product.name
        domain_location = self.database.get_named_location(domain)
        dataset_title = f'NEON {data_product_name} at {site}, {domain_location.description}'
        non_empty_string_type = eml.I18NNonEmptyStringType()
        non_empty_string_type.content = dataset_title
        self.eml.dataset.title.append(non_empty_string_type)

    def _set_creator(self) -> eml.ResponsibleParty:
        contact_eml_file = self.eml_files.get_contact()
        creator = self.xml_parser.from_string(contact_eml_file, eml.ResponsibleParty)
        self.eml.dataset.creator.clear()
        self.eml.dataset.creator.append(creator)
        # Used by other elements.
        creator_without_id = eml.ResponsibleParty()
        creator_without_id.organization_name.append(creator.organization_name[0])
        creator_without_id.individual_name.extend(creator.individual_name)
        creator_without_id.address.extend(creator.address)
        creator_without_id.phone.extend(creator.phone)
        creator_without_id.electronic_mail_address.extend(creator.electronic_mail_address)
        creator_without_id.online_url.extend(creator.online_url)
        return creator_without_id

    def _set_intellectual_rights(self):
        intellectual_rights_file = self.eml_files.get_intellectual_rights()
        text_type = self.xml_parser.from_string(intellectual_rights_file, eml.TextType)
        self.eml.dataset.intellectual_rights = text_type

    def _set_coverage(self) -> None:
        coverage = eml.Coverage()
        site_name = self.metadata.path_elements.site
        site_geometry: Geometry = self.database.get_geometry(site_name)
        if site_geometry:
            geographic_coverage = EmlCoverage(site_geometry, self.metadata, self.database).get_coverage()
            coverage.geographic_coverage = geographic_coverage
        self.eml.dataset.coverage = coverage

    def _set_citation(self, creator_without_id) -> None:
        citation = self.eml.dataset.project.study_area_description.citation[0]
        citation.creator = creator_without_id
        citation.report.publisher = creator_without_id

    def _set_dataset_id_title_dates(self) -> None:
        formats = DateFormats(self.metadata)
        start_date = formats.start_date
        end_date = formats.end_date
        start_date_dashed = formats.start_date_dashed
        end_date_dashed = formats.end_date_dashed
        dataset_id = f'{self.eml.dataset.id[0]} {start_date}-{end_date}'
        self.eml.dataset.id.clear()
        self.eml.dataset.id.append(dataset_id)
        self.eml.dataset.short_name = f'{self.eml.dataset.short_name}, {start_date_dashed} to {end_date_dashed}'
        dataset_title = f'{self.eml.dataset.title[0].content}, {start_date_dashed} to {end_date_dashed}'
        self.eml.dataset.title.clear()
        self.eml.dataset.title.append(dataset_title)
        self._set_temporal_coverage(start_date_dashed, end_date_dashed)

    def _set_temporal_coverage(self, start_date_dashed, end_date_dashed) -> None:
        begin_date = eml.SingleDateTimeType()
        end_date = eml.SingleDateTimeType()
        begin_date.calendar_date = start_date_dashed
        end_date.calendar_date = end_date_dashed
        range_of_dates = eml.TemporalCoverageRangeOfDates()
        range_of_dates.begin_date = begin_date
        range_of_dates.end_date = end_date
        temporal_coverage = eml.CoverageTemporalCoverage()
        temporal_coverage.range_of_dates = range_of_dates
        self.eml.dataset.coverage.temporal_coverage.append(temporal_coverage)

    def _set_data_tables(self) -> None:
        measurement_scale = MeasurementScale(self.publication_workbook, self.metadata, self.database)
        for file in self.metadata.data_files.files:
            data_table = eml.DataTableType()
            entity_name = Path(file.filename).stem
            data_table.entity_name = entity_name
            data_table.case_sensitive = eml.DataTableTypeCaseSensitive.YES
            data_table.number_of_records = str(file.line_count)
            attribute_list = eml.AttributeList()
            for row in self.publication_workbook.get_workbook():
                package_type = self.publication_workbook.get_download_package(row)
                if package_type != self.package_type:
                    continue
                field_name = self.publication_workbook.get_field_name(row)
                description = self.publication_workbook.get_description(row)
                attribute = eml.Attribute()
                attribute.attribute_name = field_name
                attribute.attribute_definition = description
                attribute.measurement_scale = measurement_scale.get_scale(row)
                attribute_list.attribute.append(attribute)
            data_table.attribute_list = attribute_list
            self.eml.dataset.data_table.append(data_table)

    def _set_additional_metadata(self):
        # get general NEON units metadata.
        unit_types_file = self.eml_files.get_unit_types()
        unit_types_metadata = self.xml_parser.from_string(unit_types_file, eml.EmlAdditionalMetadataMetadata)
        unit_types_additional_metadata = eml.EmlAdditionalMetadata()
        unit_types_additional_metadata.metadata = unit_types_metadata
        # get custom units from the dataset.
        custom_units_metadata = self._get_custom_units()
        if custom_units_metadata:
            custom_units_additional_metadata = eml.EmlAdditionalMetadata()
            custom_units_additional_metadata.metadata = custom_units_metadata
            # add NEON units and dataset units to the metadata element.
            self.eml.additional_metadata = [unit_types_additional_metadata, custom_units_additional_metadata]
        else:
            self.eml.additional_metadata = unit_types_additional_metadata

    def _get_custom_units(self) -> Optional[eml.EmlAdditionalMetadataMetadata]:
        neon_units = NeonUnits(self.eml_files.get_units())
        unit_list = stmml.UnitList()
        unit_names = []
        for data_table_type in self.eml.dataset.data_table:
            attribute_list = data_table_type.attribute_list
            for attributes in attribute_list.attribute:
                scale = attributes.measurement_scale
                interval = scale.interval
                ratio = scale.ratio
                unit_name = None
                if interval:
                    if interval.unit:
                        unit_name = interval.unit.custom_unit
                        if not unit_name:
                            unit_name = interval.unit.standard_unit
                if ratio:
                    if ratio.unit:
                        unit_name = ratio.unit.custom_unit
                        if not unit_name:
                            unit_name = ratio.unit.standard_unit
                if unit_name is not None and unit_name not in unit_names:  # prevent duplicates
                    unit_names.append(unit_name)
                    unit = neon_units.to_stmml(unit_name)
                    unit_list.unit.append(unit)
        if len(unit_names) > 0:
            config = SerializerConfig(pretty_print=True)
            serializer = XmlSerializer(config=config)
            string_content = serializer.render(unit_list)
            return self.xml_parser.from_string(string_content, eml.EmlAdditionalMetadataMetadata)
        return None

    def _get_filename(self) -> str:
        elements = self.metadata.path_elements
        product_id = self.metadata.data_product.short_data_product_id
        domain = self.metadata.path_elements.domain
        site = elements.site
        start = DateFormats.format_date(self.metadata.data_files.min_time)
        end = DateFormats.format_date(self.metadata.data_files.max_time)
        formatted_timestamp = format_timestamp(self.timestamp)
        return f'NEON.{domain}.{site}.{product_id}.EML.{start}-{end}.{formatted_timestamp}.xml'
