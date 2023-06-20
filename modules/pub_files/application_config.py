from pathlib import Path
from typing import NamedTuple

import environs


class ApplicationConfig(NamedTuple):
    in_path: Path
    in_path_parse_index: int
    location_path: Path
    out_path: Path
    db_secrets_path: Path
    log_level: str
    certificate_path: Path
    app_id: str
    installation_id: str
    host: str
    repo_owner: str
    branch: str
    readme_repo: str
    readme_path: str
    eml_repo: str
    eml_boilerplate_path: str
    eml_contact_path: str
    eml_intellectual_rights_path: str
    eml_unit_types_path: str
    eml_units_path: str


def configure_from_environment() -> ApplicationConfig:
    env = environs.Env()
    in_path: Path = env.path('IN_PATH')
    in_path_parse_index: int = env.int('IN_PATH_PARSE_INDEX')
    location_path: Path = env.path('LOCATION_PATH')
    out_path: Path = env.path('OUT_PATH')
    db_secrets_path: Path = env.path('DB_SECRETS_PATH')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    certificate_path: Path = env.path('GITHUB_PEM_PATH')
    app_id: str = env.str('GITHUB_APP_ID')
    installation_id: str = env.str('GITHUB_INSTALLATION_ID')
    host: str = env.str('GITHUB_HOST')
    repo_owner: str = env.str('GITHUB_REPO_OWNER')
    branch: str = env.str('GITHUB_BRANCH')
    readme_repo: str = env.str('GITHUB_README_REPO')
    readme_path: str = env.str('GITHUB_README_PATH')
    eml_repo: str = env.str('GITHUB_EML_REPO')
    eml_boilerplate_path: str = env.str('GITHUB_EML_BOILERPLATE_PATH')
    eml_contact_path: str = env.str('GITHUB_EML_CONTACT_PATH')
    eml_intellectual_rights_path: str = env.str('GITHUB_EML_INTELLECTUAL_RIGHTS_PATH')
    eml_unit_types_path: str = env.str('GITHUB_EML_UNIT_TYPES_PATH')
    eml_units_path: str = env.str('GITHUB_EML_UNITS_PATH')
    return ApplicationConfig(in_path=in_path,
                             in_path_parse_index=in_path_parse_index,
                             location_path=location_path,
                             out_path=out_path,
                             db_secrets_path=db_secrets_path,
                             log_level=log_level,
                             certificate_path=certificate_path,
                             app_id=app_id,
                             installation_id=installation_id,
                             host=host,
                             repo_owner=repo_owner,
                             branch=branch,
                             readme_repo=readme_repo,
                             readme_path=readme_path,
                             eml_repo=eml_repo,
                             eml_boilerplate_path=eml_boilerplate_path,
                             eml_contact_path=eml_contact_path,
                             eml_intellectual_rights_path=eml_intellectual_rights_path,
                             eml_unit_types_path=eml_unit_types_path,
                             eml_units_path=eml_units_path)
