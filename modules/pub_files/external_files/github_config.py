from pathlib import Path
from typing import NamedTuple, Optional


class GithubConfig(NamedTuple):
    app_id: str
    branch: Optional[str]
    certificate_path: Path
    host: str
    installation_id: str
    repo_owner: str
