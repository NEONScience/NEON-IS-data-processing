from pub_files.application_config import ApplicationConfig
from pub_files.output_files.eml.eml_file import EmlFiles
from pub_files.external_files.github_reader import GithubConfig, GithubReader


class GithubFileReader:

    def __init__(self, config: ApplicationConfig):
        github_config = GithubConfig(certificate_path=config.certificate_path,
                                     app_id=config.app_id,
                                     installation_id=config.installation_id,
                                     host=config.host,
                                     repo_owner=config.repo_owner,
                                     branch=config.branch)
        self.reader = GithubReader(github_config)
        self.config = config
        self.eml_files = EmlFiles(self.get_eml_boilerplate,
                                  self.get_eml_contact,
                                  self.get_eml_intellectual_rights,
                                  self.get_eml_unit_types)

    def get_eml_files(self):
        return self.eml_files

    def get_readme(self) -> str:
        return self.reader.read_file(self.config.readme_repo, self.config.readme_path)

    def get_workbook(self) -> str:
        return self.reader.read_file(self.config.workbook_repo, self.config.workbook_path)

    def get_eml_boilerplate(self) -> str:
        return self.reader.read_file(self.config.eml_repo, self.config.eml_boilerplate_path)

    def get_eml_contact(self) -> str:
        return self.reader.read_file(self.config.eml_repo, self.config.eml_contact_path)

    def get_eml_intellectual_rights(self) -> str:
        return self.reader.read_file(self.config.eml_repo, self.config.eml_intellectual_rights_path)

    def get_eml_unit_types(self) -> str:
        return self.reader.read_file(self.config.eml_repo, self.config.eml_unit_types_path)
