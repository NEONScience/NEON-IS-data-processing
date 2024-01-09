from pathlib import Path
from typing import NamedTuple, Optional

from pub_files.application_config import ApplicationConfig


class GithubConfig(NamedTuple):
    """Class holding the data needed to establish a connection to GitHub."""
    app_id: str
    branch: Optional[str]
    certificate_path: Path
    host: str
    installation_id: str
    repo_owner: str


def get_github_config(config: ApplicationConfig) -> GithubConfig:
    return GithubConfig(certificate_path=config.certificate_path,
                        app_id=config.app_id,
                        installation_id=config.installation_id,
                        host=config.host,
                        repo_owner=config.repo_owner,
                        branch=config.branch)
