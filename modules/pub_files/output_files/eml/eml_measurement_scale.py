from datetime import datetime
from typing import Optional, List, Callable

import eml.eml_2_2_0 as eml

from pub_files.database.publication_workbook import WorkbookRow
from pub_files.database.units import EmlUnitType
from pub_files.database.value_list import Value
from pub_files.input_files.file_metadata import FileMetadata
from pub_files.output_files.eml.eml_database import EmlDatabase


def make_get_scale(metadata: FileMetadata,
                   database: EmlDatabase) -> Callable[[WorkbookRow], Optional[eml.AttributeTypeMeasurementScale]]:

    def get_scale(row: WorkbookRow) -> Optional[eml.AttributeTypeMeasurementScale]:
        """Return the EML measurement scale using the publication workbook."""
        measurement_scale = eml.AttributeTypeMeasurementScale()
        workbook_scale = row.measurement_scale.lower()
        if workbook_scale == 'nominal':
            collect_date = metadata.data_files.min_time
            if row.lov_code != 'NA':
                if collect_date is None:
                    return
                else:
                    measurement_scale.nominal = _set_nominal(row, collect_date, database)
        elif workbook_scale == 'textdomain':
            measurement_scale.nominal = _set_text_domain(row)
        elif workbook_scale == 'interval':
            measurement_scale.interval = _set_interval(row, database)
        elif workbook_scale == 'ratio':
            measurement_scale.ratio = _set_ratio(row, metadata.path_elements.site, database)
        elif workbook_scale == 'datetime':
            date_time = eml.AttributeTypeMeasurementScaleDateTime()
            date_time.format_string = row.publication_format
            measurement_scale.date_time = date_time
        else:
            return None
        return measurement_scale

    return get_scale


def _set_nominal(row: WorkbookRow, collect_date: datetime,
                 database: EmlDatabase) -> eml.AttributeTypeMeasurementScaleNominal:
    non_numeric_domain_type = eml.NonNumericDomainType()
    enumerated_domain = eml.NonNumericDomainTypeEnumeratedDomain()
    value_list_name = row.lov_code
    values: List[Value] = database.get_value_list(value_list_name)
    for value in values:
        end_date = value.end_date
        if value.effective_date == collect_date and end_date is None or end_date == collect_date:
            code_definition = eml.NonNumericDomainTypeEnumeratedDomainCodeDefinition()
            code_definition.code = value.publication_code
            code_definition.definition = value.name
            enumerated_domain.code_definition.append(code_definition)
    non_numeric_domain_type.enumerated_domain.append(enumerated_domain)
    nominal = eml.AttributeTypeMeasurementScaleNominal()
    nominal.non_numeric_domain = non_numeric_domain_type
    return nominal


def _set_text_domain(row: WorkbookRow) -> eml.AttributeTypeMeasurementScaleNominal:
    non_numeric_domain_type = eml.NonNumericDomainType()
    text_domain = eml.NonNumericDomainTypeTextDomain()
    text_domain.definition = row.table_description
    non_numeric_domain_type.text_domain.append(text_domain)
    nominal = eml.AttributeTypeMeasurementScaleNominal()
    nominal.non_numeric_domain = non_numeric_domain_type
    return nominal


def _set_interval(row: WorkbookRow, database: EmlDatabase) -> eml.AttributeTypeMeasurementScaleInterval:
    numeric_domain_type = _get_numeric_domain_type(row)
    interval = eml.AttributeTypeMeasurementScaleInterval()
    unit_type = _get_unit_type(row, database)
    precision = _get_precision(row)
    if unit_type is not None:
        interval.unit = unit_type
    interval.numeric_domain = numeric_domain_type
    if precision is not None:
        interval.precision = precision
    return interval


def _set_ratio(row: WorkbookRow, site: str, database: EmlDatabase) -> eml.AttributeTypeMeasurementScaleRatio:
    numeric_domain_type = _get_numeric_domain_type(row)
    _set_bounds(row, site, database, numeric_domain_type)
    unit_type = _get_unit_type(row, database)
    precision = _get_precision(row)
    ratio = eml.AttributeTypeMeasurementScaleRatio()
    ratio.unit = unit_type
    ratio.numeric_domain = numeric_domain_type
    if precision is not None:
        ratio.precision = precision
    return ratio


def _get_precision(row: WorkbookRow) -> Optional[float]:
    """Return the precision based on the workbook's publication format string."""
    publication_format = row.publication_format
    if '*.#' in publication_format and 'round' in publication_format:
        hash_count = publication_format.count('#')
        precision = float(1) / (10 ** hash_count)
        return float(precision)
    return None


def _get_numeric_domain_type(row: WorkbookRow) -> eml.NumericDomainType:
    """Return the numeric domain type based on the publication workbook data type code."""
    numeric_domain_type = eml.NumericDomainType()
    data_type_code = row.data_type_code
    if data_type_code.lower() == 'integer':
        numeric_domain_type.number_type = eml.NumberType.INTEGER
    else:
        numeric_domain_type.number_type = eml.NumberType.REAL
    return numeric_domain_type


def _get_unit_type(row: WorkbookRow, database: EmlDatabase) -> Optional[eml.UnitType]:
    """Return the EML UnitType object if it is 'custom' or 'standard'."""
    workbook_unit = row.unit_name
    eml_unit_type: EmlUnitType = database.get_unit_eml_type(workbook_unit)
    unit_type = eml.UnitType()
    if eml_unit_type is None:
        return None
    if eml_unit_type.is_standard():
        unit_type.standard_unit = workbook_unit
    elif eml_unit_type.is_custom():
        unit_type.custom_unit = workbook_unit
    return unit_type


def _set_bounds(row: WorkbookRow, site: str, database: EmlDatabase, numeric_domain_type: eml.NumericDomainType) -> None:
    """Set the EML bounds based on the associated thresholds in the database."""
    has_max = False
    has_min = False
    bounds = eml.BoundsGroupBounds()
    term_name = row.field_name
    if term_name is not None:
        for threshold in database.get_thresholds(term_name):
            if threshold.location_name == site \
                    and threshold.start_day_of_year is None \
                    and threshold.end_day_of_year is None:
                value = threshold.number_value
                if threshold.threshold_name == 'Range Threshold Hard Min':
                    minimum = eml.BoundsGroupBoundsMinimum()
                    minimum.value = value
                    bounds.minimum = minimum
                    has_min = True
                elif threshold.threshold_name == 'Range Threshold Hard Max':
                    maximum = eml.BoundsGroupBoundsMaximum()
                    maximum.value = value
                    bounds.maximum = maximum
                    has_max = True
    if has_min or has_max:
        numeric_domain_type.bounds.append(bounds)
