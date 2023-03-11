import time
from pathlib import Path
from typing import Optional

import requests
import jwt


class GithubReader:

    def __init__(self, app_id: str, installation_id: str, pem_file_path: Path, host_url: str,
                 repo_owner: str, branch: Optional[str]):
        self.app_id = app_id
        self.installation_id = installation_id
        self.pem_file_path = pem_file_path
        self.host_url = host_url
        self.repo_owner = repo_owner
        self.branch = branch

    def read_file(self, repo: str, file_path: str) -> str:
        """Read a file as a string from a git repo and path."""
        jwt_token = self._get_jwt()
        access_token = self._get_app_installation_access_token(jwt_token)
        headers = {
            'Authorization': f'token {access_token}',
            'Accept': 'application/vnd.github.v3.raw+json'
        }
        url = f'{self.host_url}/repos/{self.repo_owner}/{repo}/contents/{file_path}'
        if self.branch:
            url = f'{url}?ref={self.branch}'
        response = requests.get(url, headers=headers)
        file_content = response.text
        return file_content

    def _get_jwt(self) -> str:
        """Get a JSON web token to request an app installation token from Git."""
        signing_key = jwt.jwk_from_pem(self.pem_file_path.read_bytes())
        payload = {
            'iat': int(time.time()),  # Issued at time
            'exp': int(time.time()) + 600,  # JWT expiration time (10 minutes maximum)
            'iss': self.app_id  # GitHub App's identifier
        }
        jwt_instance = jwt.JWT()
        encoded_jwt: str = jwt_instance.encode(payload, signing_key, alg='RS256')
        return encoded_jwt

    def _get_app_installation_access_token(self, jwt_token: str):
        """Request a Git app installation token from Git."""
        headers = {
            'Authorization': f'Bearer {jwt_token}',
            'Accept': 'application/vnd.github+json'
        }
        url = f'{self.host_url}/app/installations/{self.installation_id}/access_tokens'
        response = requests.post(url, headers=headers)
        response = response.json()
        installation_access_token = response['token']
        return installation_access_token
