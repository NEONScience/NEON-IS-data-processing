import os
from urllib.parse import quote

import requests
from structlog import get_logger

log = get_logger()
DEFAULT_LOV_BASE_URL = 'https://os-api-int.svcs-nonprod.gcp.neoninternal.org/os-api'


def get_lov_values(lov_name: str) -> list[dict[str, str]]:
    """Fetch LOV items formatted for CSV output rows."""
    base_url = os.environ.get('LOV_BASE_URL', DEFAULT_LOV_BASE_URL).rstrip('/')
    encoded_lov_name = quote(lov_name, safe='')
    url = f'{base_url}/list-of-values/{encoded_lov_name}'
    log.debug(f"url path is {url}")

    try:
        response = requests.get(url, headers={'Accept': 'application/json'}, timeout=15)
    except Exception as exc:
        log.warning('Failed to load LOV values', lov_name=lov_name, url=url, error=str(exc))
        return []

    if response.status_code == 200:
        payload = response.json()
    else:
        log.warning('LOV call failed', lov_name=lov_name, url=url, status_code=response.status_code)
        return []

    if isinstance(payload, dict):
        items = payload.get('listOfValuesItems') or payload.get('items', [])
    elif isinstance(payload, list):
        items = payload
    else:
        items = []

    values = []
    for item in items:
        if not isinstance(item, dict):
            continue
        pub_code = item.get('pubCode') or item.get('itemCode') or item.get('code') or ''
        description = item.get('description') or item.get('itemDescription') or ''
        start_date = item.get('effectiveDate') or item.get('startDate') or ''
        if isinstance(start_date, str):
            start_date = start_date.split('Z', 1)[0]
        end_date = item.get('endDate') or ''
        if isinstance(end_date, str):
            end_date = end_date.split('Z', 1)[0]
        if not pub_code and not description:
            continue
        values.append({'name': lov_name,
                       'pubCode': str(pub_code),
                       'description': str(description),
                       'startDate': str(start_date),
                       'endDate': str(end_date)})
    return values
