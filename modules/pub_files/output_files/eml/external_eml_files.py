from typing import NamedTuple


class ExternalEmlFiles(NamedTuple):
    """Class holding functions to read templates to create an EML metadata file."""
    boilerplate: str
    contact: str
    intellectual_rights: str
    unit_types: str
    units: str
