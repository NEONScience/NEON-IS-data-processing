from typing import NamedTuple, Callable


class ExternalEmlFiles(NamedTuple):
    """Class holding functions to read required data to populate an EML file from the database."""
    get_boilerplate: Callable[[], str]
    get_contact: Callable[[], str]
    get_intellectual_rights: Callable[[], str]
    get_unit_types: Callable[[], str]
    get_units: Callable[[], str]
