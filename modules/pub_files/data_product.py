from typing import NamedTuple


class DataProduct(NamedTuple):
    data_product_id: str
    short_data_product_id: str
    name: str
    type_name: str
    description: str
    category: str
    supplier: str
    supplier_full_name: str
    short_name: str
    abstract: str
    design_description: str
    study_description: str
    sensor: str
    basic_description: str
    expanded_description: str
    remarks: str


def build_data_product(*,
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
                       remarks: str) -> DataProduct:
    short_data_product_id = get_data_product_number(data_product_id)
    supplier_full_name = get_supplier_full_name(supplier)
    return DataProduct(data_product_id=data_product_id,
                       short_data_product_id=short_data_product_id,
                       name=name,
                       type_name=type_name,
                       description=description,
                       category=category,
                       supplier=supplier,
                       supplier_full_name=supplier_full_name,
                       short_name=short_name,
                       abstract=abstract,
                       design_description=design_description,
                       study_description=study_description,
                       sensor=sensor,
                       basic_description=basic_description,
                       expanded_description=expanded_description,
                       remarks=remarks)


def get_data_product_number(data_product_id: str) -> str:
    """Remove prefix from the given data product ID to isolate the data product number."""
    return data_product_id.replace('NEON.DOM.SITE.', '')


def get_supplier_full_name(supplier: str) -> str:
    """Return the definition for the supplier acronym."""
    if supplier == 'TIS':
        return 'Terrestrial Instrument System'
    if supplier == 'TOS':
        return 'Terrestrial Observation System'
    if supplier == 'AOP':
        return 'Airborne Observation Platform'
    if supplier == 'AOS':
        return 'Aquatic Observation System'
    if supplier == 'AIS':
        return 'Aquatic Instrument System'
    return supplier
