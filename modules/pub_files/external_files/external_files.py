from typing import NamedTuple

from pub_files.application_config import ApplicationConfig
from pub_files.external_files.github_config import get_github_config
from pub_files.external_files.github_reader import make_read_file
from pub_files.output_files.eml.external_eml_files import ExternalEmlFiles


class ExternalFiles(NamedTuple):
    readme_template: str
    eml_files: ExternalEmlFiles


def get_external_files(config: ApplicationConfig) -> ExternalFiles:
    """Return the external files read from Github."""
    github_config = get_github_config(config)
    read_file = make_read_file(github_config)
    readme_template = read_file(config.readme_repo, config.readme_path)
    eml_boilerplate = read_file(config.eml_repo, config.eml_boilerplate_path)
    eml_contact = read_file(config.eml_repo, config.eml_contact_path)
    eml_intellectual_rights = read_file(config.eml_repo, config.eml_intellectual_rights_path)
    eml_unit_types = read_file(config.eml_repo, config.eml_unit_types_path)
    eml_units = read_file(config.eml_repo, config.eml_units_path)
    eml_files = ExternalEmlFiles(eml_boilerplate, eml_contact, eml_intellectual_rights, eml_unit_types, eml_units)
    return ExternalFiles(readme_template, eml_files)
