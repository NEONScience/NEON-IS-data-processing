from typing import NamedTuple, Callable


class ExternalEmlFiles(NamedTuple):
    get_boilerplate: Callable[[], str]
    get_contact: Callable[[], str]
    get_intellectual_rights: Callable[[], str]
    get_unit_types: Callable[[], str]
