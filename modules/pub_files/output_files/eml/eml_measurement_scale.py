from typing import Optional, List

import eml.eml_2_2_0 as eml

from pub_files.database.queries.value_list import Value
from pub_files.input_files.file_metadata import FileMetadata
from pub_files.output_files.eml.eml_database import EmlDatabase
from pub_files.publication_workbook import PublicationWorkbook


class MeasurementScale:

    def __init__(self, workbook: PublicationWorkbook, metadata: FileMetadata, database: EmlDatabase):
        self.workbook = workbook
        self.metadata = metadata
        self.database = database

    def get_measurement_scale(self, row: dict) -> Optional[eml.AttributeTypeMeasurementScale]:
        measurement_scale = eml.AttributeTypeMeasurementScale()
        workbook_measurement_scale = self.workbook.get_measurement_scale(row)
        match workbook_measurement_scale.lower():
            case 'nominal':
                collect_date = self.metadata.data_files.min_time
                if self.workbook.get_os_lov(row) is not 'NA':
                    if collect_date is None:
                        return
                    else:
                        non_numeric_domain_type = eml.NonNumericDomainType()
                        enumerated_domain = eml.NonNumericDomainTypeEnumeratedDomain()
                        value_list_name = self.workbook.get_os_lov(row)
                        values: List[Value] = self.database.get_value_list(value_list_name)
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
                        measurement_scale.nominal = nominal
            case 'textdomain':
                non_numeric_domain_type = eml.NonNumericDomainType()
                text_domain = eml.NonNumericDomainTypeTextDomain()
                text_domain.definition = self.workbook.get_table_description(row)
                non_numeric_domain_type.text_domain.append(text_domain)
                nominal = eml.AttributeTypeMeasurementScaleNominal()
                nominal.non_numeric_domain = non_numeric_domain_type
                measurement_scale.nominal = nominal
            case 'interval':
                pass
            case 'ratio':
                numeric_domain_type = self.get_numeric_domain_type(row)
                self.set_bounds(row, numeric_domain_type)
                unit_type = self.get_unit_type(row)
                precision = self.get_precision(row)
                match workbook_measurement_scale.lower():
                    case 'interval':
                        interval = eml.AttributeTypeMeasurementScaleInterval()
                        interval.unit = unit_type
                        interval.numeric_domain = numeric_domain_type
                        if precision is not None:
                            interval.precision = precision
                        measurement_scale.interval = interval
                    case 'ratio':
                        ratio = eml.AttributeTypeMeasurementScaleRatio()
                        ratio.unit = unit_type
                        ratio.numeric_domain = numeric_domain_type
                        if precision is not None:
                            ratio.precision = precision
                        measurement_scale.ratio = ratio
            case 'datetime':
                date_time = eml.AttributeTypeMeasurementScaleDateTime()
                date_time.format_string = self.workbook.get_publication_format(row)
                measurement_scale.date_time = date_time
            case _:
                return None
        return measurement_scale

    def get_numeric_domain_type(self, row) -> eml.NumericDomainType:
        numeric_domain_type = eml.NumericDomainType()
        data_type_code = self.workbook.get_data_type(row)
        if data_type_code.lower() == 'integer':
            numeric_domain_type.number_type = eml.NumberType.INTEGER
        else:
            numeric_domain_type.number_type = eml.NumberType.REAL
        return numeric_domain_type

    def set_bounds(self, row, numeric_domain_type: eml.NumericDomainType) -> None:
        has_max = False
        has_min = False
        bounds = eml.BoundsGroupBounds()
        term_name = self.workbook.get_field_name(row)
        if term_name is not None:
            for threshold in self.database.get_thresholds(term_name):
                if threshold.location_name == self.metadata.path_elements.site \
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

    def get_unit_type(self, row) -> eml.UnitType:
        workbook_unit = self.workbook.get_unit(row)
        eml_type = self.database.get_unit_eml_type(workbook_unit)
        unit_type = eml.UnitType()
        if eml_type == 'standard':
            unit_type.standard_unit = workbook_unit
        elif eml_type == 'custom':
            unit_type.custom_unit = workbook_unit
        return unit_type

    def get_precision(self, row) -> Optional[float]:
        publication_format = self.workbook.get_publication_format(row)
        precision = None
        if '*.#' in publication_format and 'round' in publication_format:
            hash_count = publication_format.count('#')
            one = float(1)
            precision = one / one * (10 ** hash_count)
        return precision
