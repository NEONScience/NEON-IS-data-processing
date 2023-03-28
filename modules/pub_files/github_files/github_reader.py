import time
from pathlib import Path
from typing import Optional, NamedTuple

import requests
import jwt


class GithubConfig(NamedTuple):
    app_id: str
    branch: Optional[str]
    certificate_path: Path
    host: str
    installation_id: str
    repo_owner: str


class GithubReader:

    def __init__(self, config: GithubConfig):
        self.config = config
        self.jwt_token = self._get_jwt()
        self.access_token = self._get_app_installation_access_token(self.jwt_token)

    def read_file(self, repo: str, file_path: str) -> str:
        """Read a file as a string from a git repo and path."""
        headers = {
            'Authorization': f'token {self.access_token}',
            'Accept': 'application/vnd.github.v3.raw+json'
        }
        url = f'{self.config.host}/repos/{self.config.repo_owner}/{repo}/contents/{file_path}'
        if self.config.branch:
            url = f'{url}?ref={self.config.branch}'
        response = requests.get(url, headers=headers)
        return response.text

    def _get_jwt(self) -> str:
        """Get a JSON web token to request an app installation token from Git."""
        signing_key = jwt.jwk_from_pem(self.config.certificate_path.read_bytes())
        payload = {
            'iat': int(time.time()),  # Issued at time
            'exp': int(time.time()) + 600,  # JWT expiration time (10 minutes maximum)
            'iss': self.config.app_id  # GitHub App's identifier
        }
        jwt_instance = jwt.JWT()
        return jwt_instance.encode(payload, signing_key, alg='RS256')

    def _get_app_installation_access_token(self, jwt_token: str):
        """Request a Git app installation token from Git."""
        headers = {
            'Authorization': f'Bearer {jwt_token}',
            'Accept': 'application/vnd.github+json'
        }
        url = f'{self.config.host}/app/installations/{self.config.installation_id}/access_tokens'
        response = requests.post(url, headers=headers)
        return response.json()['token']
