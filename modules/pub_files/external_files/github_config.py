from pathlib import Path
from typing import NamedTuple, Optional


class GithubConfig(NamedTuple):
    """Class holding the data needed to establish a connection to Github."""
    app_id: str
    branch: Optional[str]
    certificate_path: Path
    host: str
    installation_id: str
    repo_owner: str
