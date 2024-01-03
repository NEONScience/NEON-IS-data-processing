import time
from typing import Callable

import requests
from jwt import JWT, jwk_from_pem

from pub_files.external_files.github_config import GithubConfig


def make_read_file(config: GithubConfig) -> Callable[[str, str], str]:
    jwt_token = _get_jwt(config)
    access_token = _get_app_installation_access_token(config, jwt_token)
    access_token_headers = {
        'Authorization': f'token {access_token}',
        'Accept': 'application/vnd.github.v3.raw+json'
    }

    def read_file(repo: str, file_path: str) -> str:
        """Read a file as a string from the given git repo and file path."""
        url = f'{config.host}/repos/{config.repo_owner}/{repo}/contents/{file_path}'
        if config.branch:
            url = f'{url}?ref={config.branch}'
        response = requests.get(url, headers=access_token_headers)
        if response.status_code != 200:
            raise RuntimeError(f'Could not read {url} from Github: {response.status_code}.')
        return response.text

    return read_file

def _get_jwt(config: GithubConfig) -> str:
    """Get a JSON Web Token (JWT) to request an app installation access token from Git."""
    signing_key = jwk_from_pem(config.certificate_path.read_bytes())
    payload = {
        'iat': int(time.time()),  # Issued at time
        'exp': int(time.time()) + 600,  # JWT expiration time (10 minutes maximum)
        'iss': config.app_id  # GitHub App's identifier
    }
    return JWT().encode(payload, signing_key, alg='RS256')

def _get_app_installation_access_token(config: GithubConfig, jwt_token: str):
    """Request a Git App installation token from GitHub."""
    headers = {
        'Authorization': f'Bearer {jwt_token}',
        'Accept': 'application/vnd.github+json'
    }
    url = f'{config.host}/app/installations/{config.installation_id}/access_tokens'
    response = requests.post(url, headers=headers)
    return response.json()['token']
