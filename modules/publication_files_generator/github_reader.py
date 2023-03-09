"""
Module to read files from a Git-hub app.
"""
import time
from pathlib import Path
from typing import NamedTuple, Optional, Callable

import requests
import jwt


class GithubConfig(NamedTuple):
    app_id: str
    installation_id: str
    pem_file_path: Path
    host_url: str
    owner: str
    branch: Optional[str]


def get_read_file(config: GithubConfig) -> Callable[[str, str], str]:
    """Closure function to hide config from read_file function."""
    def f(repo: str, file_path: str) -> str:
        return _read_file(config, repo, file_path)
    return f


def _read_file(config: GithubConfig, repo: str, file_path: str) -> str:
    """Read a file as a string from a git repo and path."""
    jwt_token = _get_jwt(config)
    access_token = _get_app_installation_access_token(jwt_token, config)
    headers = {
        'Authorization': f'token {access_token}',
        'Accept': 'application/vnd.github.v3.raw+json'
    }
    url = f'{config.host_url}/repos/{config.owner}/{repo}/contents/{file_path}'
    if config.branch:
        url = f'{url}?ref={config.branch}'
    response = requests.get(url, headers=headers)
    file_content = response.text
    return file_content


def _get_jwt(config: GithubConfig) -> str:
    """Get a JSON web token to request an app installation token from Git."""
    signing_key = jwt.jwk_from_pem(config.pem_file_path.read_bytes())
    payload = {
        'iat': int(time.time()),  # Issued at time
        'exp': int(time.time()) + 600,  # JWT expiration time (10 minutes maximum)
        'iss': config.app_id  # GitHub App's identifier
    }
    jwt_instance = jwt.JWT()
    encoded_jwt: str = jwt_instance.encode(payload, signing_key, alg='RS256')
    return encoded_jwt


def _get_app_installation_access_token(jwt_token: str, config: GithubConfig):
    """Request a Git app installation token from Git."""
    headers = {
        'Authorization': f'Bearer {jwt_token}',
        'Accept': 'application/vnd.github+json'
    }
    url = f'{config.host_url}/app/installations/{config.installation_id}/access_tokens'
    response = requests.post(url, headers=headers)
    response = response.json()
    installation_access_token = response['token']
    return installation_access_token
