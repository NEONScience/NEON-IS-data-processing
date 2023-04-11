from pathlib import Path

import environs


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
        self.eml_repo: str = env.str('GITHUB_EML_REPO')
        self.eml_boilerplate_path: str = env.str('GITHUB_EML_BOILERPLATE_PATH')
        self.eml_contact_path: str = env.str('GITHUB_EML_CONTACT_PATH')
        self.eml_intellectual_rights_path: str = env.str('GITHUB_EML_INTELLECTUAL_RIGHTS_PATH')
        self.eml_unit_types_path: str = env.str('GITHUB_EML_UNIT_TYPES_PATH')
        self.eml_units_path: str = env.str('GITHUB_EML_UNITS_PATH')
