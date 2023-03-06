from pathlib import Path

import environs

from readme_generator.generator import Paths
from readme_generator.github_app_reader import GithubConfig


class ApplicationConfig:
    """Class to hold the required application inputs for generating a readme file."""

    def __init__(self):
        env = environs.Env()
        self.in_path: Path = env.path('IN_PATH')
        self.path_parse_index: int = env.int('PATH_PARSE_INDEX')
        self.out_path: Path = env.path('OUT_PATH')
        self.db_secrets_path: Path = env.path('DB_SECRETS_PATH')
        self.log_level: str = env.log_level('LOG_LEVEL', 'INFO')
        self.github_certificate_path: Path = env.path('GITHUB_PEM')
        self.github_app_id: str = env.str('GITHUB_APP_ID')
        self.github_installation_id: str = env.str('GITHUB_INSTALLATION_ID')
        self.github_host: str = env.str('GITHUB_HOST')
        self.github_repo_owner: str = env.str('GITHUB_REPO_OWNER')
        self.github_repo: str = env.str('GITHUB_REPO')
        self.github_file_path: str = env.str('GITHUB_README_TEMPLATE_PATH')
        self.github_branch: str = env.str('GITHUB_BRANCH')

    def get_github_config(self) -> GithubConfig:
        return GithubConfig(
            pem_file_path=self.github_certificate_path,
            app_id=self.github_app_id,
            installation_id=self.github_installation_id,
            host_url=self.github_host,
            owner=self.github_repo_owner,
            repo=self.github_repo,
            file_path=self.github_file_path,
            branch=self.github_branch)

    def get_paths(self) -> Paths:
        return Paths(in_path=self.in_path,
                     out_path=self.out_path,
                     path_parse_index=self.path_parse_index)
