from pathlib import Path

import environs

from publication_files.github_reader import GithubConfig, GithubReader


class ApplicationConfig:

    def __init__(self):
        env = environs.Env()
        self.in_path: Path = env.path('IN_PATH')
        self.in_path_parse_index: int = env.int('IN_PATH_PARSE_INDEX')
        self.location_path: Path = env.path('LOCATION_PATH')
        self.out_path: Path = env.path('OUT_PATH')
        self.db_secrets_path: Path = env.path('DB_SECRETS_PATH')
        self.log_level: str = env.log_level('LOG_LEVEL', 'INFO')
        self.certificate_path: Path = env.path('GITHUB_PEM')
        self.app_id: str = env.str('GITHUB_APP_ID')
        self.installation_id: str = env.str('GITHUB_INSTALLATION_ID')
        self.host: str = env.str('GITHUB_HOST')
        self.repo_owner: str = env.str('GITHUB_REPO_OWNER')
        self.branch: str = env.str('GITHUB_BRANCH')
        self.readme_repo: str = env.str('GITHUB_README_REPO')
        self.readme_path: str = env.str('GITHUB_README_PATH')
        self.workbook_repo: str = env.str('GITHUB_PUBLICATION_WORKBOOK_REPO')
        self.workbook_path: str = env.str('GITHUB_PUBLICATION_WORKBOOK_PATH')
        self.reader = self._get_github_reader()

    def get_readme(self) -> str:
        return self.reader.read_file(self.readme_repo, self.readme_path)

    def get_workbook(self) -> str:
        return self.reader.read_file(self.workbook_repo, self.workbook_path)

    def _get_github_reader(self) -> GithubReader:
        config = GithubConfig(certificate_path=self.certificate_path,
                              app_id=self.app_id,
                              installation_id=self.installation_id,
                              host=self.host,
                              repo_owner=self.repo_owner,
                              branch=self.branch)
        return GithubReader(config)
