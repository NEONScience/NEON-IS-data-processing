import json
import os
from functools import lru_cache
from urllib.parse import quote
from urllib.request import urlopen

from structlog import get_logger

log = get_logger()
DEFAULT_LOV_BASE_URL = 'https://os-api-int.svcs-nonprod.gcp.neoninternal.org/os-api'


@lru_cache(maxsize=256)
def get_lov_values(lov_name: str) -> list[dict[str, str]]:
    """Fetch code/description LOV items for a workbook lovName."""
    base_url = os.environ.get('LOV_BASE_URL', DEFAULT_LOV_BASE_URL).rstrip('/')
    encoded_lov_name = quote(lov_name, safe='')
    url = f'{base_url}/list-of-values/{encoded_lov_name}'
    try:
        with urlopen(url, timeout=15) as response:
            payload = json.loads(response.read().decode('utf-8'))
    except Exception as exc:
        log.warning('Failed to load LOV values', lov_name=lov_name, url=url, error=str(exc))
        return []

    if isinstance(payload, dict):
        items = payload.get('items', [])
    elif isinstance(payload, list):
        items = payload
    else:
        items = []

    values = []
    for item in items:
        if not isinstance(item, dict):
            continue
        code = item.get('itemCode') or item.get('code') or ''
        description = item.get('itemDescription') or item.get('description') or ''
        if not code and not description:
            continue
        values.append({'code': str(code), 'description': str(description)})
    return values
