"""
Module to read files from a Github app.
"""
import time
from pathlib import Path
from typing import NamedTuple

import requests
import jwt


class GithubConfig(NamedTuple):
    app_id: str
    installation_id: str
    pem_file_path: Path
    host_url: str
    owner: str
    repo: str
    file_path: str
    branch: str | None


def get_jwt(config: GithubConfig) -> str:
    signing_key = jwt.jwk_from_pem(config.pem_file_path.read_bytes())
    payload = {
        'iat': int(time.time()),  # Issued at time
        'exp': int(time.time()) + 600,  # JWT expiration time (10 minutes maximum)
        'iss': config.app_id  # GitHub App's identifier
    }
    jwt_instance = jwt.JWT()
    encoded_jwt: str = jwt_instance.encode(payload, signing_key, alg='RS256')
    return encoded_jwt


def get_app_installation_access_token(jwt_token: str, config: GithubConfig):
    headers = {
        'Authorization': f'Bearer {jwt_token}',
        'Accept': 'application/vnd.github+json'
    }
    url = f'{config.host_url}/app/installations/{config.installation_id}/access_tokens'
    response = requests.post(url, headers=headers)
    response = response.json()
    installation_access_token = response['token']
    return installation_access_token


def read_file(config: GithubConfig) -> str:
    jwt_token = get_jwt(config)
    access_token = get_app_installation_access_token(jwt_token, config)
    headers = {
        'Authorization': f'token {access_token}',
        'Accept': 'application/vnd.github.v3.raw+json'
    }
    url = f'{config.host_url}/repos/{config.owner}/{config.repo}/contents/{config.file_path}'
    if config.branch:
        url = f'{url}?ref={config.branch}'
    response = requests.get(url, headers=headers)
    file_content = response.text
    return file_content
