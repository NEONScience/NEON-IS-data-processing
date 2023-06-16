from datetime import datetime
from pathlib import Path
from typing import Optional, NamedTuple

import eml.eml_2_2_0 as eml
from xsdata.formats.dataclass.parsers import XmlParser
from xsdata.formats.dataclass.serializers import XmlSerializer
from xsdata.formats.dataclass.serializers.config import SerializerConfig

import pub_files.output_files.eml.date_formats as date_formats
import pub_files.output_files.eml.stmml.stmml_1_2 as stmml
from pub_files.database.publication_workbook import PublicationWorkbook
from pub_files.geometry import Geometry
from pub_files.input_files.file_metadata import FileMetadata
from pub_files.output_files.eml.eml_coverage import get_geographic_coverage
from pub_files.output_files.eml.eml_database import EmlDatabase
from pub_files.output_files.eml.eml_measurement_scale import make_get_scale
from pub_files.output_files.eml.external_eml_files import ExternalEmlFiles
from pub_files.output_files.eml.neon_units import NeonUnits
from pub_files.output_files.filename_format import format_timestamp


class EmlFileConfig(NamedTuple):
    out_path: Path
    metadata: FileMetadata
    eml_templates: ExternalEmlFiles
    workbook: PublicationWorkbook
    package_type: str
    timestamp: datetime
    database: EmlDatabase


def write_eml_file(config: EmlFileConfig) -> Path:
    xml_parser = XmlParser()
    eml_boilerplate_content = config.eml_templates.boilerplate
    file_eml: eml.Eml = xml_parser.from_string(eml_boilerplate_content, eml.Eml)
    add_file_content(file_eml, config, xml_parser)
    filename = get_filename(config.metadata, config.timestamp)
    path = Path(config.out_path, filename)
    file_content: str = render_content(file_eml)
    path.write_text(file_content)
    return path


def add_file_content(file_eml: eml.Eml, config: EmlFileConfig, xml_parser: XmlParser) -> None:
    """Populate the Eml object representing the file content."""
    set_dataset_id(file_eml, config)
    set_dataset_title(file_eml, config)
    creator = set_creator(file_eml, config, xml_parser)
    creator_without_id = get_creator_without_id(creator)
    file_eml.dataset.metadata_provider.clear()
    file_eml.dataset.metadata_provider.append(creator_without_id)
    file_eml.dataset.pub_date = date_formats.format_dashed_date(config.timestamp)
    file_eml.dataset.language = eml.I18NNonEmptyStringType().content = 'English'
    file_eml.dataset.purpose = config.metadata.data_product.description
    file_eml.dataset.contact = creator_without_id
    file_eml.dataset.publisher = creator_without_id
    set_intellectual_rights(file_eml, config, xml_parser)
    set_coverage(file_eml, config)
    set_citation(file_eml, creator_without_id)
    set_dataset_id_title_dates(file_eml, config)
    set_data_tables(file_eml, config)
    set_additional_metadata(file_eml, config, xml_parser)


def render_content(file_eml: eml.Eml) -> str:
    """Render the Eml object into an XML string."""
    config = SerializerConfig(pretty_print=True)
    serializer = XmlSerializer(config=config)
    return serializer.render(file_eml)


def set_dataset_id(file_eml: eml.Eml, config: EmlFileConfig) -> None:
    """Add the dataset identifier to the EML dataset."""
    file_eml.dataset.id.append(config.metadata.path_elements.data_product_id)
    file_eml.dataset.short_name = f'NEON {config.metadata.path_elements.site} {config.metadata.data_product.name}'


def set_dataset_title(file_eml: eml.Eml, config: EmlFileConfig) -> None:
    """Add the title to the EML dataset."""
    site = config.metadata.path_elements.site
    domain = config.metadata.path_elements.domain
    data_product_name = config.metadata.data_product.name
    domain_location = config.database.get_named_location(domain)
    dataset_title = f'NEON {data_product_name} at {site}, {domain_location.description}'
    non_empty_string_type = eml.I18NNonEmptyStringType()
    non_empty_string_type.content = dataset_title
    file_eml.dataset.title.append(non_empty_string_type)


def set_creator(file_eml: eml.Eml, config: EmlFileConfig, xml_parser: XmlParser) -> eml.ResponsibleParty:
    """Add the creator to the EML dataset."""
    contact_eml_file = config.eml_templates.contact
    creator = xml_parser.from_string(contact_eml_file, eml.ResponsibleParty)
    file_eml.dataset.creator.clear()
    file_eml.dataset.creator.append(creator)
    return creator


def get_creator_without_id(creator: eml.ResponsibleParty) -> eml.ResponsibleParty:
    """The creator without id is needed by other elements."""
    creator_without_id = eml.ResponsibleParty()
    creator_without_id.organization_name.append(creator.organization_name[0])
    creator_without_id.individual_name.extend(creator.individual_name)
    creator_without_id.address.extend(creator.address)
    creator_without_id.phone.extend(creator.phone)
    creator_without_id.electronic_mail_address.extend(creator.electronic_mail_address)
    creator_without_id.online_url.extend(creator.online_url)
    return creator_without_id


def set_intellectual_rights(file_eml: eml.Eml, config: EmlFileConfig, xml_parser: XmlParser) -> None:
    """Add the intellectual rights to the EML dataset."""
    intellectual_rights_file = config.eml_templates.intellectual_rights
    text_type = xml_parser.from_string(intellectual_rights_file, eml.TextType)
    file_eml.dataset.intellectual_rights = text_type


def set_coverage(file_eml: eml.Eml, config: EmlFileConfig) -> None:
    """Add the geographic coverage to the EML dataset."""
    coverage = eml.Coverage()
    site_name = config.metadata.path_elements.site
    site_geometry: Geometry = config.database.get_geometry(site_name)
    if site_geometry:
        unit_name = config.database.get_spatial_unit(site_geometry.srid)
        geographic_coverage = get_geographic_coverage(site_geometry, config.metadata, unit_name)
        coverage.geographic_coverage = geographic_coverage
    file_eml.dataset.coverage = coverage


def set_citation(file_eml: eml.Eml, creator_without_id: eml.ResponsibleParty) -> None:
    """Add the EML creator as the EML citation to the dataset."""
    citation = file_eml.dataset.project.study_area_description.citation[0]
    citation.creator = creator_without_id
    citation.report.publisher = creator_without_id


def set_dataset_id_title_dates(file_eml: eml.Eml, config: EmlFileConfig) -> None:
    """Add the title to the dataset."""
    start_date = date_formats.format_date(config.metadata.data_files.min_time)
    end_date = date_formats.format_date(config.metadata.data_files.max_time)
    start_date_dashed = date_formats.format_dashed_date(config.metadata.data_files.min_time)
    end_date_dashed = date_formats.format_dashed_date(config.metadata.data_files.max_time)
    dataset_id = f'{file_eml.dataset.id[0]} {start_date}-{end_date}'
    file_eml.dataset.id.clear()
    file_eml.dataset.id.append(dataset_id)
    file_eml.dataset.short_name = f'{file_eml.dataset.short_name}, {start_date_dashed} to {end_date_dashed}'
    dataset_title = f'{file_eml.dataset.title[0].content}, {start_date_dashed} to {end_date_dashed}'
    file_eml.dataset.title.clear()
    file_eml.dataset.title.append(dataset_title)
    set_temporal_coverage(file_eml, start_date_dashed, end_date_dashed)


def set_temporal_coverage(file_eml: eml.Eml, start_date_dashed, end_date_dashed) -> None:
    """Add the EML temporal coverage to the dataset."""
    begin_date = eml.SingleDateTimeType()
    end_date = eml.SingleDateTimeType()
    begin_date.calendar_date = start_date_dashed
    end_date.calendar_date = end_date_dashed
    range_of_dates = eml.TemporalCoverageRangeOfDates()
    range_of_dates.begin_date = begin_date
    range_of_dates.end_date = end_date
    temporal_coverage = eml.CoverageTemporalCoverage()
    temporal_coverage.range_of_dates = range_of_dates
    file_eml.dataset.coverage.temporal_coverage.append(temporal_coverage)


def set_data_tables(file_eml: eml.Eml, config: EmlFileConfig) -> None:
    """Add the data tables to the EML dataset."""
    get_scale = make_get_scale(config.metadata, config.database)
    for file in config.metadata.data_files.files:
        data_table = eml.DataTableType()
        entity_name = Path(file.filename).stem
        data_table.entity_name = entity_name
        data_table.case_sensitive = eml.DataTableTypeCaseSensitive.YES
        data_table.number_of_records = str(file.line_count)
        attribute_list = eml.AttributeList()
        for row in config.workbook.rows:
            package_type = row.download_package
            if package_type != config.package_type:
                continue
            field_name = row.field_name
            description = row.description
            attribute = eml.Attribute()
            attribute.attribute_name = field_name
            attribute.attribute_definition = description
            attribute.measurement_scale = get_scale(row)
            if attribute not in attribute_list.attribute:
                attribute_list.attribute.append(attribute)
        data_table.attribute_list = attribute_list
        file_eml.dataset.data_table.append(data_table)


def set_additional_metadata(file_eml: eml.Eml, config: EmlFileConfig, xml_parser: XmlParser) -> None:
    """Add the additional metadata section to the EML document."""
    # get general NEON units metadata.
    unit_types_file = config.eml_templates.unit_types
    unit_types_metadata = xml_parser.from_string(unit_types_file, eml.EmlAdditionalMetadataMetadata)
    unit_types_additional_metadata = eml.EmlAdditionalMetadata()
    unit_types_additional_metadata.metadata = unit_types_metadata
    # get custom units from the dataset.
    custom_units_metadata = get_custom_units(file_eml, config, xml_parser)
    if custom_units_metadata:
        custom_units_additional_metadata = eml.EmlAdditionalMetadata()
        custom_units_additional_metadata.metadata = custom_units_metadata
        # add NEON units and dataset units to the metadata element.
        file_eml.additional_metadata = [unit_types_additional_metadata, custom_units_additional_metadata]
    else:
        file_eml.additional_metadata = unit_types_additional_metadata


def get_custom_units(file_eml: eml.Eml, config: EmlFileConfig,
                     xml_parser: XmlParser) -> Optional[eml.EmlAdditionalMetadataMetadata]:
    """Add the NEON custom units as EML additional metadata."""
    neon_units = NeonUnits(config.eml_templates.units)
    unit_list = stmml.UnitList()
    unit_names = []
    for data_table_type in file_eml.dataset.data_table:
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
        return xml_parser.from_string(string_content, eml.EmlAdditionalMetadataMetadata)
    return None


def get_filename(metadata: FileMetadata, timestamp: datetime) -> str:
    """Return the EML filename."""
    elements = metadata.path_elements
    product_id = metadata.data_product.short_data_product_id
    domain = metadata.path_elements.domain
    site = elements.site
    start = date_formats.format_date(metadata.data_files.min_time)
    end = date_formats.format_date(metadata.data_files.max_time)
    formatted_timestamp = format_timestamp(timestamp)
    return f'NEON.{domain}.{site}.{product_id}.EML.{start}-{end}.{formatted_timestamp}.xml'
