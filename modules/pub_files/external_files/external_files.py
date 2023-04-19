from pub_files.application_config import ApplicationConfig
from pub_files.external_files.github_config import GithubConfig
from pub_files.external_files.github_reader import GithubReader
from pub_files.output_files.eml.external_eml_files import ExternalEmlFiles


class ExternalFiles:

    def __init__(self, config: ApplicationConfig):
        github_config = GithubConfig(certificate_path=config.certificate_path,
                                     app_id=config.app_id,
                                     installation_id=config.installation_id,
                                     host=config.host,
                                     repo_owner=config.repo_owner,
                                     branch=config.branch)
        reader = GithubReader(github_config)
        self.readme_template = reader.read_file(config.readme_repo, config.readme_path)
        self.publication_workbook = reader.read_file(config.workbook_repo, config.workbook_path)
        self.eml_boilerplate = reader.read_file(config.eml_repo, config.eml_boilerplate_path)
        self.eml_contact = reader.read_file(config.eml_repo, config.eml_contact_path)
        self.eml_intellectual_rights = reader.read_file(config.eml_repo, config.eml_intellectual_rights_path)
        self.eml_unit_types = reader.read_file(config.eml_repo, config.eml_unit_types_path)
        self.eml_units = reader.read_file(config.eml_repo, config.eml_units_path)
        self.eml_files = ExternalEmlFiles(self._get_eml_boilerplate,
                                          self._get_eml_contact,
                                          self._get_eml_intellectual_rights,
                                          self._get_eml_unit_types,
                                          self._get_eml_units)

    def get_eml_files(self):
        return self.eml_files

    def get_readme(self) -> str:
        return self.readme_template

    def get_workbook(self) -> str:
        return self.publication_workbook

    def _get_eml_boilerplate(self) -> str:
        return self.eml_boilerplate

    def _get_eml_contact(self) -> str:
        return self.eml_contact

    def _get_eml_intellectual_rights(self) -> str:
        return self.eml_intellectual_rights

    def _get_eml_unit_types(self) -> str:
        return self.eml_unit_types

    def _get_eml_units(self) -> str:
        return self.eml_units
