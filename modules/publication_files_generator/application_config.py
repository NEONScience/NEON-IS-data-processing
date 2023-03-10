from pathlib import Path
from typing import NamedTuple

import environs


class ApplicationConfig(NamedTuple):
    in_path: Path
    in_path_parse_index: int
    out_path: Path
    db_secrets_path: Path
    log_level: str
    github_certificate_path: Path
    github_app_id: str
    github_installation_id: str
    github_host: str
    github_repo_owner: str
    github_branch: str
    github_readme_repo: str
    github_readme_path: str
    github_publication_workbook_repo: str
    github_publication_workbook_path: str


def read_environment() -> ApplicationConfig:
    env = environs.Env()
    in_path: Path = env.path('IN_PATH')
    in_path_parse_index: int = env.int('IN_PATH_PARSE_INDEX')
    out_path: Path = env.path('OUT_PATH')
    db_secrets_path: Path = env.path('DB_SECRETS_PATH')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    github_certificate_path: Path = env.path('GITHUB_PEM')
    github_app_id: str = env.str('GITHUB_APP_ID')
    github_installation_id: str = env.str('GITHUB_INSTALLATION_ID')
    github_host: str = env.str('GITHUB_HOST')
    github_repo_owner: str = env.str('GITHUB_REPO_OWNER')
    github_branch: str = env.str('GITHUB_BRANCH')
    github_readme_repo: str = env.str('GITHUB_README_REPO')
    github_readme_path: str = env.str('GITHUB_README_PATH')
    github_publication_workbook_repo: str = env.str('GITHUB_PUBLICATION_WORKBOOK_REPO')
    github_publication_workbook_path: str = env.str('GITHUB_PUBLICATION_WORKBOOK_PATH')
    return ApplicationConfig(in_path=in_path,
                             in_path_parse_index=in_path_parse_index,
                             out_path=out_path,
                             db_secrets_path=db_secrets_path,
                             log_level=log_level,
                             github_certificate_path=github_certificate_path,
                             github_app_id=github_app_id,
                             github_installation_id=github_installation_id,
                             github_host=github_host,
                             github_repo_owner=github_repo_owner,
                             github_branch=github_branch,
                             github_readme_repo=github_readme_repo,
                             github_readme_path=github_readme_path,
                             github_publication_workbook_repo=github_publication_workbook_repo,
                             github_publication_workbook_path=github_publication_workbook_path)
