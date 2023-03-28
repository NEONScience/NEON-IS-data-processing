from pub_files.config import ApplicationConfig
from pub_files.github_files.github_reader import GithubConfig, GithubReader


class GithubFileReader:

    def __init__(self, config: ApplicationConfig):
        github_config = GithubConfig(certificate_path=config.certificate_path,
                                     app_id=config.app_id,
                                     installation_id=config.installation_id,
                                     host=config.host,
                                     repo_owner=config.repo_owner,
                                     branch=config.branch)
        self.reader = GithubReader(github_config)
        self.readme_repo = config.readme_repo
        self.readme_path = config.readme_path
        self.workbook_repo = config.workbook_repo
        self.workbook_path = config.workbook_path
        self.eml_repo = config.eml_repo
        self.eml_path = config.eml_path

    def get_readme(self) -> str:
        return self.reader.read_file(self.readme_repo, self.readme_path)

    def get_workbook(self) -> str:
        return self.reader.read_file(self.workbook_repo, self.workbook_path)

    def get_eml(self) -> str:
        return self.reader.read_file(self.eml_repo, self.eml_path)
