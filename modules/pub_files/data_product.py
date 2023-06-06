
def get_data_product_number(data_product_id: str) -> str:
    """Remove 'NEON.DOM.SITE.' from a data product ID to isolate the data product number."""
    return data_product_id.replace('NEON.DOM.SITE.', '')


def get_term_data_product_number(data_product_id: str) -> str:
    """Convert horizontal and vertical indices into wildcards."""
    parts = data_product_id.split('.')
    parts[7] = 'HOR'
    parts[8] = 'VER'
    return '.'.join(parts)

class DataProduct:
    """Class to consolidate data product data."""

    def __init__(self,
                 data_product_id: str,
                 name: str,
                 type_name: str,
                 description: str,
                 category: str,
                 supplier: str,
                 short_name: str,
                 abstract: str,
                 design_description: str,
                 study_description: str,
                 sensor: str,
                 basic_description: str,
                 expanded_description: str,
                 remarks: str):
        self.data_product_id = data_product_id
        self.short_data_product_id = get_data_product_number(self.data_product_id)
        self.name = name
        self.type_name = type_name
        self.description = description
        self.category = category
        self.supplier = supplier
        self.supplier_full_name = self._get_supplier_full_name()
        self.short_name = short_name
        self.abstract = abstract
        self.design_description = design_description
        self.study_description = study_description
        self.sensor = sensor
        self.basic_description = basic_description
        self.expanded_description = expanded_description
        self.remarks = remarks

    def _get_supplier_full_name(self) -> str:
        """Return the supplier acronym definition."""
        if self.supplier == 'TIS':
            return 'Terrestrial Instrument System'
        if self.supplier == 'TOS':
            return 'Terrestrial Observation System'
        if self.supplier == 'AOP':
            return 'Airborne Observation Platform'
        if self.supplier == 'AOS':
            return 'Aquatic Observation System'
        if self.supplier == 'AIS':
            return 'Aquatic Instrument System'
        return self.supplier
